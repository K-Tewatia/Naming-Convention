import streamlit as st
import pandas as pd
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json
import io
import os
from datetime import datetime
import time
import hashlib

from supabase import create_client, Client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# State Persistence Functions
# -----------------------
def get_user_id():
    """Generate a persistent user ID based on browser"""
    if "user_id" not in st.session_state:
        # Try to get from query params first (for persistence across refreshes)
        query_params = st.query_params
        if "user_id" in query_params:
            st.session_state.user_id = query_params["user_id"]
        else:
            # Create a new unique ID and set it in query params
            from streamlit.runtime.scriptrunner import get_script_run_ctx
            session_id = get_script_run_ctx().session_id
            new_user_id = hashlib.md5(session_id.encode()).hexdigest()
            st.session_state.user_id = new_user_id
            st.query_params["user_id"] = new_user_id
    return st.session_state.user_id

def save_state_to_supabase():
    """Save current session state to Supabase"""
    try:
        supabase = get_supabase_client()
        user_id = get_user_id()
        
        state_data = {
            "user_id": user_id,
            "pending_changes": st.session_state.pending_changes,
            "index": st.session_state.index,
            "last_updated": datetime.now().isoformat(),
            "excel_filename": st.session_state.get("uploaded_excel_name", None)
        }
        
        # Check if record exists
        existing = supabase.table("user_states").select("id").eq("user_id", user_id).execute()
        
        if existing.data and len(existing.data) > 0:
            # Update existing record
            supabase.table("user_states").update({
                "state_data": json.dumps(state_data),
                "last_updated": datetime.now().isoformat()
            }).eq("user_id", user_id).execute()
        else:
            # Insert new record
            supabase.table("user_states").insert({
                "user_id": user_id,
                "state_data": json.dumps(state_data),
                "last_updated": datetime.now().isoformat()
            }).execute()
        
        return True
    except Exception as e:
        # Silently log errors to avoid disrupting user experience
        print(f"Failed to save state: {e}")
        return False

def load_state_from_supabase():
    """Load session state from Supabase"""
    try:
        supabase = get_supabase_client()
        user_id = get_user_id()
        
        response = supabase.table("user_states").select("*").eq("user_id", user_id).execute()
        
        if response.data and len(response.data) > 0:
            state_data = json.loads(response.data[0]["state_data"])
            
            # Restore state
            st.session_state.pending_changes = state_data.get("pending_changes", {})
            st.session_state.index = state_data.get("index", 0)
            st.session_state.uploaded_excel_name = state_data.get("excel_filename", None)
            
            return True
        return False
    except Exception as e:
        print(f"Could not load previous state: {e}")
        return False

def auto_refresh_script():
    """JavaScript to auto-refresh page after 5 minutes of inactivity"""
    return """
    <script>
    let inactivityTimer;
    const INACTIVITY_TIMEOUT = 5 * 60 * 1000; // 5 minutes in milliseconds
    
    function resetTimer() {
        clearTimeout(inactivityTimer);
        inactivityTimer = setTimeout(() => {
            window.location.reload();
        }, INACTIVITY_TIMEOUT);
    }
    
    // Reset timer on any user activity
    ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart', 'click'].forEach(event => {
        document.addEventListener(event, resetTimer, true);
    });
    
    // Initialize timer
    resetTimer();
    </script>
    """

# -----------------------
# Page Config and Auto-refresh
# -----------------------
st.set_page_config(page_title="📂 File Rename Validator", layout="wide")

# Inject auto-refresh script
st.components.v1.html(auto_refresh_script(), height=0)

st.title("📂 File Rename Validator")

# -----------------------
# Initialize session state
# -----------------------
if "pending_changes" not in st.session_state:
    st.session_state.pending_changes = {}
if "index" not in st.session_state:
    st.session_state.index = 0
if "drive_service" not in st.session_state:
    st.session_state.drive_service = None
if "file_cache" not in st.session_state:
    st.session_state.file_cache = {}
if "df" not in st.session_state:
    st.session_state.df = None
if "invalid_rows" not in st.session_state:
    st.session_state.invalid_rows = None
if "state_loaded" not in st.session_state:
    st.session_state.state_loaded = False
if "last_save_time" not in st.session_state:
    st.session_state.last_save_time = time.time()

# Load previous state on first run
if not st.session_state.state_loaded:
    if load_state_from_supabase():
        st.session_state.state_loaded = True
        # Show restoration message
        if st.session_state.pending_changes:
            st.sidebar.success(f"🔄 Restored {len(st.session_state.pending_changes)} pending changes")
    else:
        st.session_state.state_loaded = True

# Auto-save state periodically (every 30 seconds if there are changes)
current_time = time.time()
if current_time - st.session_state.last_save_time > 30:
    if st.session_state.pending_changes:
        save_state_to_supabase()
    st.session_state.last_save_time = current_time

# -----------------------
# Sidebar uploads
# -----------------------
st.sidebar.header("📁 Upload Files")
credentials_file = st.secrets["gcp_service_account"]

excel_file = st.sidebar.file_uploader("Upload Excel File", type=["xlsx"])

# Store uploaded Excel filename
if excel_file and "uploaded_excel_name" not in st.session_state:
    st.session_state.uploaded_excel_name = excel_file.name

# Show pending changes info in sidebar
if st.session_state.pending_changes:
    st.sidebar.markdown("---")
    st.sidebar.info(f"💾 {len(st.session_state.pending_changes)} pending changes (auto-saved)")

# -----------------------
# Helper functions with caching
# -----------------------
@st.cache_resource
def build_drive_service_from_secrets(_gcp_credentials):
    """Build Drive service from secrets.toml credentials"""
    creds = service_account.Credentials.from_service_account_info(
        _gcp_credentials,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


@st.cache_data(ttl=3600)
def find_folder_id(_drive_service, folder_name, parent_id=None):
    """Cached folder lookup - reduces repeated API calls"""
    try:
        q_parts = [
            "name = '" + folder_name.replace("'", "\\'") + "'",
            "mimeType = 'application/vnd.google-apps.folder'",
            "trashed = false"
        ]
        if parent_id:
            q_parts.append(f"'{parent_id}' in parents")
        q = " and ".join(q_parts)
        resp = _drive_service.files().list(q=q, fields="files(id, name)", pageSize=5).execute()
        items = resp.get("files", [])
        if items:
            return items[0]["id"]
    except Exception:
        return None
    return None

@st.cache_data(ttl=3600)
def get_file_in_folder(_drive_service, parent_folder_id, filename):
    """Cached file lookup with webViewLink"""
    try:
        q = (
            "name = '" + filename.replace("'", "\\'") + 
            "' and '" + parent_folder_id + "' in parents and trashed = false"
        )
        resp = _drive_service.files().list(
            q=q, 
            fields="files(id, name, mimeType, webViewLink)", 
            pageSize=10
        ).execute()
        items = resp.get("files", [])
        if items:
            return items[0]
    except Exception:
        return None
    return None

def download_file_to_temp(drive_service, file_id, file_name_hint="file"):
    """Downloads file from Drive to temp location"""
    try:
        meta = drive_service.files().get(
            fileId=file_id, 
            fields="mimeType, name, webViewLink"
        ).execute()
        mime = meta.get("mimeType")
        web_view_link = meta.get("webViewLink")
        suffix = os.path.splitext(meta.get("name", file_name_hint))[1]
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        request = drive_service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(tmp, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        tmp.close()
        return tmp.name, mime, meta.get("name"), web_view_link
    except Exception as e:
        return None, None, None, None

def get_file_from_drive(drive_service, row, file_cache):
    """Smart file retrieval with caching"""
    cache_key = f"{row['Full Path']}_{row['Original Name']}"
    
    # Check cache first
    if cache_key in file_cache:
        return file_cache[cache_key]
    
    # Build path segments
    base_segments = ["Cog Culture Repository", "Clients", "Aarize Group"]
    raw_path = str(row["Full Path"])
    extra_segments = [seg for seg in raw_path.replace("\\", "/").strip("/").split("/") if seg]
    
    # Remove base if already in path
    lower_extra = [s.lower() for s in extra_segments]
    base_low = [s.lower() for s in base_segments]
    start_idx = None
    for i in range(len(lower_extra)):
        if lower_extra[i:i+len(base_low)] == base_low:
            start_idx = i + len(base_low)
            break
    
    file_path_segments = extra_segments[start_idx:] if start_idx is not None else extra_segments
    
    # Traverse folders
    parent_id = None
    for seg in base_segments:
        found = find_folder_id(drive_service, seg, parent_id=parent_id)
        if not found:
            parent_id = find_folder_id(drive_service, seg, parent_id=None)
        else:
            parent_id = found
        if not parent_id:
            break
    
    if not parent_id:
        parent_id = find_folder_id(drive_service, base_segments[0], parent_id=None)
    
    if parent_id:
        for seg in file_path_segments[:-1]:
            folder_id = find_folder_id(drive_service, seg, parent_id=parent_id)
            if folder_id:
                parent_id = folder_id
            else:
                break
    
    filename_guess = file_path_segments[-1] if file_path_segments else row["Original Name"]
    
    # Try to find file
    file_meta = None
    tmp_path = None
    mime = None
    actual_name = None
    web_view_link = None
    
    if parent_id:
        file_meta = get_file_in_folder(drive_service, parent_id, filename_guess)
        if file_meta:
            tmp_path, mime, actual_name, web_view_link = download_file_to_temp(
                drive_service, file_meta["id"], filename_guess
            )
    
    if not file_meta:
        try:
            q = "name = '" + filename_guess.replace("'", "\\'") + "' and trashed = false"
            resp = drive_service.files().list(
                q=q, 
                fields="files(id, name, mimeType, webViewLink)", 
                pageSize=5
            ).execute()
            items = resp.get("files", [])
            if items:
                file_meta = items[0]
                tmp_path, mime, actual_name, web_view_link = download_file_to_temp(
                    drive_service, file_meta["id"], filename_guess
                )
        except Exception:
            pass
    
    if not file_meta:
        try:
            orig = str(row["Original Name"])
            q = "name = '" + orig.replace("'", "\\'") + "' and trashed = false"
            resp = drive_service.files().list(
                q=q, 
                fields="files(id, name, mimeType, webViewLink)", 
                pageSize=5
            ).execute()
            items = resp.get("files", [])
            if items:
                file_meta = items[0]
                tmp_path, mime, actual_name, web_view_link = download_file_to_temp(
                    drive_service, file_meta["id"], orig
                )
        except Exception:
            pass
    
    # Always return 5 values (file_meta, tmp_path, mime, actual_name, web_view_link)
    result = (file_meta, tmp_path, mime, actual_name, web_view_link)
    file_cache[cache_key] = result
    return result

def save_pending_changes_to_excel(df, pending_changes, excel_filename="Updated_Clients_Rename_Log.xlsx"):
    
    # Apply pending name changes
    for full_path, new_name in pending_changes.items():
        mask = df["Full Path"] == full_path
        if mask.sum() == 0:
            mask = df["Original Name"] == full_path
        df.loc[mask, "Proposed New Name"] = new_name

    # Save the changes to the **existing** Excel file
    df.to_excel(excel_filename, index=False)

    # Store the last saved filename for later upload
    st.session_state["last_saved_file"] = excel_filename

    # Save state after saving Excel
    save_state_to_supabase()

    st.success(f"✅ Changes saved to the existing file: {excel_filename}")
    return excel_filename

def upload_to_supabase(file_path, bucket_name="renamed_excel"):
    """Uploads file to Supabase storage, replacing if already exists"""
    try:
        supabase = get_supabase_client()
        file_name = os.path.basename(file_path)

        # Try to remove existing file first
        try:
            remove_response = supabase.storage.from_(bucket_name).remove([file_name])
            st.info(f"🔄 Removed existing file: {file_name}")
        except Exception as remove_error:
            # File might not exist, which is fine
            print(f"Remove attempt: {remove_error}")

        # Upload the file
        with open(file_path, "rb") as f:
            file_content = f.read()
            
        upload_response = supabase.storage.from_(bucket_name).upload(
            path=file_name,
            file=file_content,
            file_options={
                "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "upsert": "true"
            }
        )
        
        st.success(f"📤 File successfully uploaded to Supabase: {file_name}")
        return True

    except Exception as e:
        st.error(f"⚠️ Upload to Supabase failed: {str(e)}")
        # Show more details for debugging
        st.error(f"Details: {type(e).__name__}")
        return False

# -----------------------
# Main app logic
# -----------------------
if credentials_file and excel_file:
    # Build drive service once
    if st.session_state.drive_service is None:
        try:
            st.session_state.drive_service = build_drive_service(credentials_file)
        except Exception as e:
            st.error(f"Auth error: {e}")
            st.stop()
    
    drive_service = st.session_state.drive_service
    
    # Load Excel once
    if st.session_state.df is None:
        df = pd.read_excel(excel_file)
        required_cols = ["Type", "Original Name", "Proposed New Name", "Full Path", "Created Date", "Timestamp", "Action"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            st.error(f"Missing columns: {missing}")
            st.stop()
        
        # Find invalid rows
        placeholders = ["Brand", "Campaign", "Channel", "Asset", "Format", "Version", "Date"]
        invalid_mask = df["Proposed New Name"].astype(str).apply(
            lambda s: any(ph.lower() == part.lower() for ph in placeholders for part in str(s).split("_"))
        )
        invalid_rows = df[invalid_mask].reset_index(drop=True)
        
        st.session_state.df = df
        st.session_state.invalid_rows = invalid_rows
    
    df = st.session_state.df
    invalid_rows = st.session_state.invalid_rows
    
    # Display metrics
    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.metric("Total Files", len(df))
    with col_m2:
        st.metric("Files Flagged", len(invalid_rows))
    with col_m3:
        st.metric("Pending Changes", len(st.session_state.pending_changes))
    
    if len(invalid_rows) == 0:
        st.success("✅ All proposed names look good!")
        st.stop()
    
    # Navigation
    col_nav1, col_nav2, col_nav3 = st.columns([1, 6, 1])
    with col_nav1:
        if st.button("⬅️ Previous") and st.session_state.index > 0:
            st.session_state.index -= 1
            save_state_to_supabase()
            st.rerun()
    with col_nav3:
        if st.button("Next ➡️") and st.session_state.index < len(invalid_rows) - 1:
            st.session_state.index += 1
            save_state_to_supabase()
            st.rerun()
    
    st.markdown(f"### File {st.session_state.index + 1} of {len(invalid_rows)}")
    st.progress((st.session_state.index + 1) / len(invalid_rows))
    
    row = invalid_rows.iloc[st.session_state.index]
    
    # Pre-fetch file info for Drive link
    with st.spinner("Loading file info..."):
        file_meta, tmp_path, mime, actual_name, web_view_link = get_file_from_drive(
            drive_service, row, st.session_state.file_cache
        )
    
    # -----------------------
    # THREE COLUMN LAYOUT
    # -----------------------
    col_left, col_middle, col_right = st.columns([2, 3, 3])
    
    # LEFT: Original Name Info + View in Drive Button
    with col_left:
        st.markdown("#### 📄 Current File Info")
        st.text_input("Original Name", value=row['Original Name'], disabled=True)
        st.text_input("Current Proposed", value=row['Proposed New Name'], disabled=True)
        st.text_area("Full Path", value=row['Full Path'], height=100, disabled=True)
        st.text_input("Created Date", value=str(row.get('Created Date', '')), disabled=True)
        
        # Show if this file has pending changes
        cache_key = row['Full Path']
        if cache_key in st.session_state.pending_changes:
            st.info(f"✏️ **Pending:** {st.session_state.pending_changes[cache_key]}")
        
        # View in Drive button
        st.markdown("---")
        if web_view_link:
            st.link_button(
                "🔗 View in Google Drive",
                web_view_link,
                use_container_width=True,
                type="primary"
            )
        else:
            st.warning("⚠️ Drive link not available")
    
    # MIDDLE: Edit Interface
    with col_middle:
        st.markdown("#### ✏️ Edit Default Fields")
        
        current_name = st.session_state.pending_changes.get(row['Full Path'], str(row["Proposed New Name"]))
        parts = current_name.split("_")
        while len(parts) < 7:
            parts.append("")
        
        fields = ["Brand", "Campaign", "Channel", "Asset", "Format", "Version", "Date"]
        edited_parts = []
        
        for i, field in enumerate(fields):
            current = parts[i] if i < len(parts) else ""
            if current.strip().lower() == field.lower():
                val = st.text_input(f"{field} ⚠️ (needs update)", value=current, key=f"field_{i}_{st.session_state.index}")
                edited_parts.append(val)
            else:
                st.text_input(f"{field}", value=current, disabled=True, key=f"field_locked_{i}_{st.session_state.index}")
                edited_parts.append(current)
        
        new_proposed = "_".join(edited_parts)
        st.markdown("**Preview:**")
        st.code(new_proposed, language=None)
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("💾 Save Change", use_container_width=True):
                st.session_state.pending_changes[row['Full Path']] = new_proposed
                save_state_to_supabase()
                st.success("✅ Change saved to batch!")
                st.rerun()
        
        with col_btn2:
            if st.button("🔄 Reset", use_container_width=True):
                if row['Full Path'] in st.session_state.pending_changes:
                    del st.session_state.pending_changes[row['Full Path']]
                save_state_to_supabase()
                st.rerun()
        
        # Auto-save every 10 files
        if len(st.session_state.pending_changes) >= 10:
            st.warning(f"⚠️ {len(st.session_state.pending_changes)} changes pending - save recommended!")
            if st.button("💾 Save Batch to Excel Now", use_container_width=True, type="primary"):
                out_fname = save_pending_changes_to_excel(df, st.session_state.pending_changes)
                num_changes = len(st.session_state.pending_changes)
                st.session_state.pending_changes.clear()
                save_state_to_supabase()
                st.success(f"✅ {num_changes} changes saved to {out_fname}")
                with open(out_fname, "rb") as f:
                    st.download_button(
                        "📥 Download Updated Excel",
                        data=f,
                        file_name=out_fname,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                st.rerun()
    
    # RIGHT: File Preview
    with col_right:
        st.markdown("#### 👀 File Preview")
        
        if file_meta and tmp_path:
            try:
                if mime and mime.startswith("image/"):
                    st.image(tmp_path, use_container_width=True)
                elif mime and mime.startswith("video/"):
                    st.video(tmp_path)
                elif mime and mime.startswith("audio/"):
                    st.audio(tmp_path)
                elif mime == "application/pdf":
                    with open(tmp_path, "rb") as f:
                        pdf_bytes = f.read()
                    st.download_button(
                        "📄 Open PDF",
                        data=pdf_bytes,
                        file_name=actual_name,
                        use_container_width=True
                    )
                else:
                    with open(tmp_path, "rb") as f:
                        file_bytes = f.read()
                    st.download_button(
                        "📥 Download File",
                        data=file_bytes,
                        file_name=actual_name,
                        use_container_width=True
                    )
            except Exception as e:
                st.error(f"Preview error: {e}")
        else:
            st.warning("⚠️ File not found in Drive")
    
    # -----------------------
    # Bottom: Batch Actions
    # -----------------------
    st.markdown("---")
    st.markdown("### 📊 Pending Changes Summary")
    
    if st.session_state.pending_changes:
        changes_df = pd.DataFrame([
            {"Original Path": k, "New Name": v}
            for k, v in st.session_state.pending_changes.items()
        ])
        st.dataframe(changes_df, use_container_width=True)
        
        col_action1, col_action2 = st.columns(2)
        with col_action1:
            if st.button("💾 Save All to Excel", use_container_width=True, type="primary"):
                out_fname = save_pending_changes_to_excel(df, st.session_state.pending_changes)
                num_changes = len(st.session_state.pending_changes)
                st.session_state.pending_changes.clear()
                save_state_to_supabase()
                st.success(f"✅ All {num_changes} changes saved to {out_fname}")
                with open(out_fname, "rb") as f:
                    st.download_button(
                        "📥 Download Updated Excel",
                        data=f,
                        file_name=out_fname,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            # Upload button (new)
            if st.button("⬆️ Upload Last Saved Excel to Supabase", use_container_width=True, type="secondary"):
                last_saved = st.session_state.get("last_saved_file", None)
                if last_saved and os.path.exists(last_saved):
                    upload_to_supabase(last_saved)
                else:
                    st.warning("⚠️ Please save an Excel file first before uploading.")

        with col_action2:
            if st.button("🗑️ Clear All Pending", use_container_width=True):
                st.session_state.pending_changes.clear()
                save_state_to_supabase()
                st.rerun()
    else:
        st.info("No pending changes. Make edits and click 'Save Change' to queue them.")
else:
    st.info("⬅️ Upload both Service Account JSON and Excel file to begin")
    st.markdown("""
    ### Features:
    - **3-column layout**: Original info | Edit interface | File preview
    - **View in Drive button**: Direct link to open file in Google Drive
    - **Batch saving**: Changes saved to Excel every 10 files or manually
    - **Performance**: Caching for API calls and file downloads
    - **Smart tracking**: See pending changes before committing
    - **No Drive changes**: All edits only affect Excel file
    - **🆕 State Persistence**: Your progress is automatically saved and restored
    - **🆕 Auto-refresh**: Page refreshes after 5 minutes of inactivity
    """)