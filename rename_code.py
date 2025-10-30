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

# -----------------------
# Configuration from Secrets
# -----------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
BUCKET_NAME = st.secrets["BUCKET_NAME"]  # e.g., "brand_excel_files"
BRAND_FOLDER = st.secrets["BRAND_FOLDER"]  # e.g., "Aarize_Group" or "Brand_2"
ORIGINAL_EXCEL_NAME = st.secrets.get("ORIGINAL_EXCEL_NAME", "Clients_Rename_Log.xlsx")

@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Supabase Storage Functions
# -----------------------
def download_file_from_supabase(bucket_name, file_path):
    """Download a file from Supabase storage"""
    try:
        supabase = get_supabase_client()
        file_data = supabase.storage.from_(bucket_name).download(file_path)
        return file_data
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

def upload_file_to_supabase(local_file_path, bucket_name, destination_path):
    """Upload a file to Supabase storage"""
    try:
        supabase = get_supabase_client()
        
        # Read file content
        with open(local_file_path, "rb") as f:
            file_content = f.read()
        
        # Try to remove existing file first (for updates)
        try:
            supabase.storage.from_(bucket_name).remove([destination_path])
            print(f"Removed existing file: {destination_path}")
        except Exception as e:
            print(f"No existing file to remove: {e}")
        
        # Upload the file
        upload_response = supabase.storage.from_(bucket_name).upload(
            path=destination_path,
            file=file_content,
            file_options={
                "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "upsert": "true"
            }
        )
        
        return True
    except Exception as e:
        st.error(f"Upload error: {e}")
        return False

def get_original_excel_from_brand():
    """Download the original Excel file from brand folder"""
    try:
        file_path = f"{BRAND_FOLDER}/{ORIGINAL_EXCEL_NAME}"
        file_data = download_file_from_supabase(BUCKET_NAME, file_path)
        
        if file_data:
            local_path = f"temp_original_{BRAND_FOLDER}_{ORIGINAL_EXCEL_NAME}"
            with open(local_path, "wb") as f:
                f.write(file_data)
            return local_path
        else:
            st.error(f"❌ Could not find Excel file at: {file_path}")
            return None
    except Exception as e:
        st.error(f"Could not load original Excel: {e}")
        return None

def get_updated_excel_from_brand():
    """Download the updated Excel file from brand folder if it exists"""
    try:
        updated_filename = f"{ORIGINAL_EXCEL_NAME.replace('.xlsx', '')}_updated.xlsx"
        file_path = f"{BRAND_FOLDER}/{updated_filename}"
        file_data = download_file_from_supabase(BUCKET_NAME, file_path)
        
        if file_data:
            local_path = f"temp_updated_{BRAND_FOLDER}_{updated_filename}"
            with open(local_path, "wb") as f:
                f.write(file_data)
            return local_path
        return None
    except Exception as e:
        print(f"No updated Excel found (normal for first run): {e}")
        return None

# -----------------------
# State Persistence Functions
# -----------------------
def get_user_id():
    """Generate a persistent user ID based on browser"""
    if "user_id" not in st.session_state:
        query_params = st.query_params
        if "user_id" in query_params:
            st.session_state.user_id = query_params["user_id"]
        else:
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
            "brand": BRAND_FOLDER,
            "pending_changes": st.session_state.pending_changes,
            "index": st.session_state.index,
            "last_updated": datetime.now().isoformat(),
            "total_saves": st.session_state.get("total_saves", 0)
        }
        
        # Use brand-specific state tracking
        state_key = f"{user_id}_{BRAND_FOLDER}"
        existing = supabase.table("user_states").select("id").eq("user_id", state_key).execute()
        
        if existing.data and len(existing.data) > 0:
            supabase.table("user_states").update({
                "state_data": json.dumps(state_data),
                "last_updated": datetime.now().isoformat()
            }).eq("user_id", state_key).execute()
        else:
            supabase.table("user_states").insert({
                "user_id": state_key,
                "state_data": json.dumps(state_data),
                "last_updated": datetime.now().isoformat()
            }).execute()
        
        return True
    except Exception as e:
        print(f"Failed to save state: {e}")
        return False

def load_state_from_supabase():
    """Load session state from Supabase"""
    try:
        supabase = get_supabase_client()
        user_id = get_user_id()
        state_key = f"{user_id}_{BRAND_FOLDER}"
        
        response = supabase.table("user_states").select("*").eq("user_id", state_key).execute()
        
        if response.data and len(response.data) > 0:
            state_data = json.loads(response.data[0]["state_data"])
            
            st.session_state.pending_changes = state_data.get("pending_changes", {})
            st.session_state.index = state_data.get("index", 0)
            st.session_state.total_saves = state_data.get("total_saves", 0)
            
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
    const INACTIVITY_TIMEOUT = 5 * 60 * 1000;
    
    function resetTimer() {
        clearTimeout(inactivityTimer);
        inactivityTimer = setTimeout(() => {
            window.location.reload();
        }, INACTIVITY_TIMEOUT);
    }
    
    ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart', 'click'].forEach(event => {
        document.addEventListener(event, resetTimer, true);
    });
    
    resetTimer();
    </script>
    """

# -----------------------
# Page Config
# -----------------------
st.set_page_config(page_title=f"📂 File Rename Validator - {BRAND_FOLDER}", layout="wide")
st.components.v1.html(auto_refresh_script(), height=0)

st.title(f"📂 File Rename Validator - {BRAND_FOLDER}")

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
if "total_saves" not in st.session_state:
    st.session_state.total_saves = 0
if "working_excel_path" not in st.session_state:
    st.session_state.working_excel_path = None
if "excel_loaded" not in st.session_state:
    st.session_state.excel_loaded = False

# Load previous state on first run
if not st.session_state.state_loaded:
    if load_state_from_supabase():
        st.session_state.state_loaded = True
        if st.session_state.pending_changes or st.session_state.index > 0:
            st.sidebar.success(f"🔄 Restored: File {st.session_state.index + 1}, {len(st.session_state.pending_changes)} changes")
    else:
        st.session_state.state_loaded = True

# Auto-save state periodically
current_time = time.time()
if current_time - st.session_state.last_save_time > 30:
    if st.session_state.pending_changes or st.session_state.index > 0:
        save_state_to_supabase()
    st.session_state.last_save_time = current_time

# -----------------------
# Sidebar Info
# -----------------------
st.sidebar.header("🏢 Brand Information")
st.sidebar.info(f"**Brand:** {BRAND_FOLDER}")
st.sidebar.info(f"**Excel:** {ORIGINAL_EXCEL_NAME}")

if st.session_state.pending_changes:
    st.sidebar.markdown("---")
    st.sidebar.info(f"💾 {len(st.session_state.pending_changes)} pending changes")

if st.session_state.total_saves > 0:
    st.sidebar.info(f"📊 Total saves: {st.session_state.total_saves}")

# -----------------------
# Helper functions with caching
# -----------------------
@st.cache_resource
def build_drive_service_from_secrets(_gcp_credentials):
    """Build and cache Google Drive service using Streamlit secrets"""
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
    
    result = (file_meta, tmp_path, mime, actual_name, web_view_link)
    file_cache[cache_key] = result
    return result

def get_working_excel_path():
    """Get the current working Excel file path"""
    # Try to download updated Excel from Supabase first
    if st.session_state.total_saves > 0:
        downloaded_path = get_updated_excel_from_brand()
        if downloaded_path and os.path.exists(downloaded_path):
            return downloaded_path
    
    # Otherwise, return the working path if it exists
    if st.session_state.working_excel_path and os.path.exists(st.session_state.working_excel_path):
        return st.session_state.working_excel_path
    
    return None

def save_pending_changes_to_excel(df, pending_changes):
    """Save pending changes to Excel, building on previous saves"""
    try:
        # Get the current working Excel path
        working_path = get_working_excel_path()
        
        # If we have a working path, load it to get all previous changes
        if working_path:
            df = pd.read_excel(working_path)
        
        # Apply pending name changes to the DataFrame
        for full_path, new_name in pending_changes.items():
            mask = df["Full Path"] == full_path
            if mask.sum() == 0:
                mask = df["Original Name"] == full_path
            df.loc[mask, "Proposed New Name"] = new_name
        
        # Create output filename
        updated_filename = f"{ORIGINAL_EXCEL_NAME.replace('.xlsx', '')}_updated.xlsx"
        local_output_path = f"temp_{BRAND_FOLDER}_{updated_filename}"
        
        # Save to local file
        df.to_excel(local_output_path, index=False)
        
        # Upload to Supabase in the brand folder
        supabase_path = f"{BRAND_FOLDER}/{updated_filename}"
        upload_success = upload_file_to_supabase(local_output_path, BUCKET_NAME, supabase_path)
        
        if upload_success:
            # Update working path
            st.session_state.working_excel_path = local_output_path
            
            # Increment save counter
            st.session_state.total_saves += 1
            
            # Save state
            save_state_to_supabase()
            
            return local_output_path
        else:
            st.error("Failed to upload to Supabase")
            return None
    except Exception as e:
        st.error(f"Error saving Excel: {e}")
        return None

# -----------------------
# Main app logic
# -----------------------

# Build drive service
credentials_file = st.secrets["gcp_service_account"]

if st.session_state.drive_service is None:
    try:
        st.session_state.drive_service = build_drive_service_from_secrets(credentials_file)
    except Exception as e:
        st.error(f"Auth error: {e}")
        st.stop()

drive_service = st.session_state.drive_service

# Load Excel automatically from Supabase
if st.session_state.df is None and not st.session_state.excel_loaded:
    with st.spinner(f"📥 Loading Excel from Supabase ({BRAND_FOLDER})..."):
        # Try to get working Excel first (if previous saves exist)
        working_path = get_working_excel_path()
        
        if working_path:
            df = pd.read_excel(working_path)
            st.sidebar.success("✅ Loaded previous working file")
        else:
            # Load original Excel from Supabase
            original_path = get_original_excel_from_brand()
            if not original_path:
                st.error(f"❌ Could not load Excel file from Supabase folder: {BRAND_FOLDER}")
                st.stop()
            
            df = pd.read_excel(original_path)
            st.sidebar.success("✅ Loaded original Excel")
            
            # Save as initial working copy
            updated_filename = f"{ORIGINAL_EXCEL_NAME.replace('.xlsx', '')}_updated.xlsx"
            initial_working_path = f"temp_{BRAND_FOLDER}_{updated_filename}"
            df.to_excel(initial_working_path, index=False)
            st.session_state.working_excel_path = initial_working_path
        
        st.session_state.excel_loaded = True
        
        # Validate required columns
        required_cols = ["Type", "Original Name", "Proposed New Name", "Full Path", "Created Date", "Timestamp", "Action"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            st.error(f"Missing required columns: {missing}")
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
col_m1, col_m2, col_m3, col_m4 = st.columns(4)
with col_m1:
    st.metric("Total Files", len(df))
with col_m2:
    st.metric("Files Flagged", len(invalid_rows))
with col_m3:
    st.metric("Pending Changes", len(st.session_state.pending_changes))
with col_m4:
    st.metric("Current Position", f"{st.session_state.index + 1}/{len(invalid_rows)}")

if len(invalid_rows) == 0:
    st.success("✅ All proposed names look good!")
    st.balloons()
    st.stop()

# Ensure index is within bounds
if st.session_state.index >= len(invalid_rows):
    st.session_state.index = len(invalid_rows) - 1
if st.session_state.index < 0:
    st.session_state.index = 0

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
    
    # Auto-save warning every 10 files
    if len(st.session_state.pending_changes) >= 10:
        st.warning(f"⚠️ {len(st.session_state.pending_changes)} changes pending - save recommended!")
        if st.button("💾 Save Batch Now", use_container_width=True, type="primary"):
            out_fname = save_pending_changes_to_excel(df, st.session_state.pending_changes)
            if out_fname:
                num_changes = len(st.session_state.pending_changes)
                st.session_state.pending_changes.clear()
                save_state_to_supabase()
                st.success(f"✅ {num_changes} changes saved & uploaded to Supabase!")
                with open(out_fname, "rb") as f:
                    st.download_button(
                        "📥 Download Updated Excel",
                        data=f,
                        file_name=os.path.basename(out_fname),
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
        if st.button("💾 Save All & Upload to Supabase", use_container_width=True, type="primary"):
            out_fname = save_pending_changes_to_excel(df, st.session_state.pending_changes)
            if out_fname:
                num_changes = len(st.session_state.pending_changes)
                st.session_state.pending_changes.clear()
                save_state_to_supabase()
                st.success(f"✅ All {num_changes} changes saved & uploaded to Supabase!")
                with open(out_fname, "rb") as f:
                    st.download_button(
                        "📥 Download Updated Excel",
                        data=f,
                        file_name=os.path.basename(out_fname),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
    
    with col_action2:
        if st.button("🗑️ Clear All Pending", use_container_width=True):
            st.session_state.pending_changes.clear()
            save_state_to_supabase()
            st.rerun()
else:
    st.info("No pending changes. Make edits and click 'Save Change' to queue them.")
