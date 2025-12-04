# Job Attachments Deep Dive: asset_sync.py, download.py, upload.py

## Overview

This document provides a deep analysis of the core Job Attachments modules that handle synchronization, downloading, and uploading of assets to/from S3.

---

## Module: asset_sync.py

### Purpose
Manages job-level attachment synchronization between local filesystem and S3, handling both input downloads and output uploads for worker sessions.

### Class: AssetSync

#### Constructor
```python
AssetSync(
    farm_id: str,
    boto3_session: Optional[boto3.Session] = None,
    manifest_version: ManifestVersion = ManifestVersion.v2023_03_03,
    deadline_endpoint_url: Optional[str] = None,
    session_id: Optional[str] = None
)
```

**Key Instance Variables:**
- `farm_id`: AWS Deadline Cloud farm identifier
- `session`: boto3 session for AWS API calls
- `s3_uploader`: S3AssetUploader instance for uploads
- `manifest_model`: Manifest model class based on version
- `synced_assets_mtime`: Dict[str, int] - Maps file paths to modification times (microseconds)
- `hash_alg`: HashAlgorithm - Default hash algorithm from manifest model
- `_local_root_to_src_map`: Dict[str, str] - Maps local roots to source paths

---

### Key Functions

#### `generate_dynamic_path_mapping(session_dir, attachments) -> dict[str, PathMappingRule]`
**Input:**
- `session_dir`: Path - Worker session directory
- `attachments`: Attachments - Job attachment configuration

**Output:** Dictionary mapping root paths to PathMappingRule objects

**Logic:**
1. Iterates through manifest properties in attachments
2. For manifests without `fileSystemLocationName`:
   - Generates unique directory name from root path
   - Creates PathMappingRule with source→destination mapping
3. Returns mapping dictionary

---

#### `attachment_sync_inputs(...) -> Tuple[SummaryStatistics, List[Dict[str, str]]]`

**Input:**
- `s3_settings`: JobAttachmentS3Settings - S3 bucket/prefix configuration
- `attachments`: Attachments - Job attachment configuration
- `queue_id`, `job_id`: Identifiers
- `session_dir`: Path - Local session directory
- `fs_permission_settings`: FileSystemPermissionSettings - File permissions
- `storage_profiles_path_mapping_rules`: Dict[str, str] - Storage profile mappings
- `step_dependencies`: Optional[list[str]] - Step IDs for step-step dependencies
- `on_downloading_files`: Callback for progress reporting
- `os_env_vars`: Environment variables for subprocesses

**Output:** Tuple of (SummaryStatistics, list of path mapping rules as dicts)

**Call Stack:**
```
attachment_sync_inputs()
│
├── generate_dynamic_path_mapping()         # Create session-relative mappings
│
├── _aggregate_asset_root_manifests()       # Fetch and merge manifests
│   │
│   ├── get_local_destination()             # Resolve local path for each manifest
│   │
│   ├── get_manifest_from_s3()              # Download input manifests
│   │
│   ├── [For step_dependencies]:
│   │   └── get_output_manifests_by_asset_root()  # Get step output manifests
│   │
│   └── merge_asset_manifests()             # Merge manifests per root
│
├── [If VIRTUAL filesystem]:
│   └── _launch_vfs()                       # Mount virtual filesystem
│       ├── VFSProcessManager.find_vfs()
│       └── mount_vfs_from_manifests()
│
├── [If COPIED filesystem]:
│   └── copied_download()                   # Download files to disk
│       ├── _ensure_disk_capacity()
│       └── download_files_from_manifests()
│
└── _record_attachment_mtimes()             # Track file modification times
```

---

#### `sync_outputs(...) -> SummaryStatistics`

**Input:**
- `s3_settings`: JobAttachmentS3Settings
- `attachments`: Attachments
- `queue_id`, `job_id`, `step_id`, `task_id`, `session_action_id`: Identifiers
- `start_time`: float - Session start timestamp
- `session_dir`: Path
- `storage_profiles_path_mapping_rules`: Dict[str, str]
- `on_uploading_files`: Progress callback

**Output:** SummaryStatistics of upload operation

**Call Stack:**
```
sync_outputs()
│
├── [For each manifest_properties]:
│   │
│   ├── Resolve local_root from storage profile or session_dir
│   │
│   ├── _get_output_files()                 # Find modified/new files
│   │   │
│   │   ├── [For each output_dir]:
│   │   │   └── glob("**/*")                # Walk directory tree
│   │   │
│   │   ├── Check synced_assets_mtime       # Detect modifications
│   │   │
│   │   ├── _is_file_within_directory()     # Security check
│   │   │
│   │   ├── hash_file()                     # Compute file hash
│   │   │
│   │   └── s3_uploader.file_already_uploaded()  # Check S3 existence
│   │
│   ├── _generate_output_manifest()         # Create manifest from OutputFiles
│   │
│   └── _upload_output_manifest_to_s3()     # Upload manifest
│       └── s3_uploader.upload_bytes_to_s3()
│
└── _upload_output_files_to_s3()            # Upload all output files
    │
    └── [For each file]:
        └── s3_uploader.upload_file_to_s3()
```

---

#### `_get_output_files(...) -> List[OutputFile]`

**Logic:**
1. For each output directory in manifest:
   - Walk directory with `glob("**/*")`
   - For each file:
     - Check if modified since last sync (compare mtime)
     - Validate file is within session directory (security)
     - Compute file hash
     - Check if already in S3
     - Create OutputFile object

**Data Structure - OutputFile:**
```python
@dataclass
class OutputFile:
    file_size: int          # Size in bytes
    file_hash: str          # Content hash
    rel_path: str           # Path relative to root (POSIX format)
    full_path: str          # Absolute local path
    s3_key: str             # S3 object key
    in_s3: bool             # Whether already uploaded
    base_dir: str           # Base directory for security checks
```

---

## Module: download.py

### Purpose
Handles downloading files from S3 Content-Addressable Storage (CAS) to local filesystem.

### Key Functions

#### `download_files_from_manifests(...) -> DownloadSummaryStatistics`

**Input:**
- `s3_bucket`: str - S3 bucket name
- `manifests_by_root`: Dict[str, BaseAssetManifest] - Manifests keyed by local root
- `cas_prefix`: Optional[str] - CAS prefix in S3
- `fs_permission_settings`: Optional[FileSystemPermissionSettings]
- `session`: Optional[boto3.Session]
- `on_downloading_files`: Progress callback
- `conflict_resolution`: FileConflictResolution

**Output:** DownloadSummaryStatistics

**Call Stack:**
```
download_files_from_manifests()
│
├── get_s3_client()
├── _get_num_download_workers()             # Calculate thread pool size
├── ProgressTracker()                       # Initialize progress tracking
│
├── [For each local_root, manifest]:
│   │
│   └── _download_files_parallel()          # Parallel download
│       │
│       ├── ThreadPoolExecutor(max_workers)
│       │
│       └── [For each file]:
│           └── download_file()             # Single file download
│               │
│               ├── Construct S3 key: {cas_prefix}/{hash}.{algorithm}
│               │
│               ├── Handle conflict resolution:
│               │   ├── SKIP: Return if exists
│               │   ├── OVERWRITE: Continue
│               │   └── CREATE_COPY: Rename to "file (N).ext"
│               │
│               ├── transfer_manager.download()  # S3 transfer
│               │
│               └── os.utime()              # Set modification time
│
└── _set_fs_group()                         # Set file permissions
```

---

#### `download_file(...) -> Tuple[int, Optional[Path]]`

**Input:**
- `file`: RelativeFilePath - File metadata from manifest
- `hash_algorithm`: HashAlgorithm
- `local_download_dir`: str
- `collision_lock`: Lock - Thread synchronization
- `collision_file_dict`: DefaultDict[str, int] - Collision counter
- `s3_bucket`, `cas_prefix`: S3 location
- `s3_client`, `session`: AWS clients
- `modified_time_override`: Optional[float]
- `progress_tracker`: Optional[ProgressTracker]
- `file_conflict_resolution`: FileConflictResolution

**Output:** Tuple of (file_bytes, local_path or None if skipped)

**S3 Key Construction:**
```python
s3_key = f"{cas_prefix}/{file.hash}.{hash_algorithm.value}"
# Example: "Data/abc123def456.xxh128"
```

**Conflict Resolution Logic:**
```python
if local_file_path.is_file():
    if SKIP:
        return (file_bytes, None)  # Don't download
    elif OVERWRITE:
        pass  # Continue, will overwrite
    elif CREATE_COPY:
        # Find unique name: "file (1).ext", "file (2).ext", etc.
        local_file_path = _get_new_copy_file_path(...)
```

---

#### `get_manifest_from_s3(manifest_key, s3_bucket, session) -> BaseAssetManifest`

**Logic:**
1. Call `s3_client.get_object()` with ExpectedBucketOwner
2. Extract asset-root from metadata
3. Decode manifest content
4. Return BaseAssetManifest object

**S3 Metadata Handling:**
```python
def _get_asset_root_from_metadata(metadata):
    if "asset-root-json" in metadata:
        return json.loads(metadata["asset-root-json"])  # Non-ASCII paths
    else:
        return metadata.get("asset-root", None)  # ASCII paths
```

---

#### `get_output_manifests_by_asset_root(...) -> dict[str, list[BaseAssetManifest]]`

**Logic:**
1. Build S3 prefix for output manifests
2. List S3 objects matching step/task pattern
3. Download manifests in parallel with ThreadPoolExecutor
4. Group by asset root
5. Merge chronologically using LastModified timestamp

---

## Module: upload.py

### Purpose
Handles uploading assets to S3 CAS, including manifest creation and file uploads.

### Class: S3AssetUploader

#### Constructor
```python
S3AssetUploader(session: Optional[boto3.Session] = None)
```

**Configuration from config_file:**
- `small_file_threshold_multiplier`: Determines small vs large file threshold
- `s3_max_pool_connections`: Max S3 connections
- `small_file_threshold`: 8MB × multiplier
- `num_upload_workers`: Calculated from pool connections

---

### Key Functions

#### `upload_assets(...) -> tuple[str, str]`

**Input:**
- `job_attachment_settings`: JobAttachmentS3Settings
- `manifest`: BaseAssetManifest
- `source_root`: Path - Local root path
- `partial_manifest_prefix`: Optional[str] - S3 key prefix
- `file_system_location_name`: Optional[str]
- `progress_tracker`: Optional[ProgressTracker]
- `s3_check_cache_dir`: Optional[str]
- `manifest_write_dir`: Optional[str]
- `manifest_name_suffix`: str (default: "input")
- `manifest_metadata`: dict
- `manifest_file_name`: Optional[str]
- `asset_root`: Optional[Path]

**Output:** Tuple of (partial_manifest_key, manifest_hash)

**Call Stack:**
```
upload_assets()
│
├── _gather_upload_metadata()               # Get hash, bytes, name
│
├── [If manifest_write_dir]:
│   └── _write_local_manifest()             # Save locally
│
├── [If partial_manifest_prefix]:
│   └── upload_bytes_to_s3()                # Upload manifest to S3
│
├── verify_hash_cache_integrity()           # Validate S3 check cache
│   └── [If invalid]: reset_s3_check_cache()
│
└── upload_input_files()                    # Upload all files
```

---

#### `upload_input_files(...)`

**Input:**
- `manifest`: BaseAssetManifest
- `s3_bucket`: str
- `source_root`: Path
- `s3_cas_prefix`: str
- `progress_tracker`: Optional[ProgressTracker]
- `s3_check_cache_dir`: Optional[str]

**Call Stack:**
```
upload_input_files()
│
├── _separate_files_by_size()               # Split small/large files
│   └── Returns (small_file_queue, large_file_queue)
│
├── S3CheckCache()                          # Open cache context
│
├── [Small files - parallel]:
│   └── ThreadPoolExecutor(num_upload_workers)
│       └── upload_object_to_cas()
│
└── [Large files - serial]:
    └── upload_object_to_cas()              # One at a time
```

**File Size Separation:**
```python
small_file_threshold = 8MB × small_file_threshold_multiplier
# Small files: parallel upload (multiple files at once)
# Large files: serial upload (one file, parallel multipart)
```

---

#### `upload_object_to_cas(...) -> Tuple[bool, int]`

**Input:**
- `file`: BaseManifestPath
- `hash_algorithm`: HashAlgorithm
- `s3_bucket`, `source_root`, `s3_cas_prefix`: Locations
- `s3_check_cache`: S3CheckCache
- `progress_tracker`: Optional[ProgressTracker]

**Output:** Tuple of (was_uploaded, file_size)

**Logic:**
```python
s3_upload_key = f"{s3_cas_prefix}/{file.hash}.{hash_algorithm.value}"

# 1. Check local cache
if s3_check_cache.get_entry(s3_key):
    return (False, file.size)  # Skip - cached

# 2. Check S3 directly
if file_already_uploaded(s3_bucket, s3_upload_key):
    # Skip - already in S3
else:
    upload_file_to_s3(...)
    is_uploaded = True

# 3. Update cache
s3_check_cache.put_entry(...)

return (is_uploaded, file.size)
```

---

#### `upload_file_to_s3(...)`

**Input:**
- `local_path`: Path
- `s3_bucket`: str
- `s3_upload_key`: str
- `progress_tracker`: Optional[ProgressTracker]
- `base_dir_path`: Optional[Path] - For security validation

**Logic:**
```python
# 1. Resolve real path (follow symlinks)
real_path = local_path.resolve()

# 2. Security checks
if base_dir_path:
    if not _is_file_within_directory(real_path, base_dir_path):
        return  # Skip - outside allowed directory

# 3. Open file safely (no symlink following)
with _open_non_symlink_file_binary(real_path) as file_obj:
    
    # 4. Upload with progress tracking
    future = transfer_manager.upload(
        fileobj=file_obj,
        bucket=s3_bucket,
        key=s3_upload_key,
        subscribers=[ProgressCallbackInvoker(handler)]
    )
    
    # 5. Wait for completion
    future.result()
```

---

### Class: S3AssetManager

#### Purpose
High-level manager for asset operations: grouping paths, creating manifests, uploading.

#### Key Functions

#### `prepare_paths_for_upload(...) -> AssetUploadGroup`

**Input:**
- `input_paths`: list[str]
- `output_paths`: list[str]
- `referenced_paths`: list[str]
- `storage_profile`: Optional[StorageProfile]
- `require_paths_exist`: bool

**Output:** AssetUploadGroup containing grouped paths and totals

**Call Stack:**
```
prepare_paths_for_upload()
│
├── _group_asset_paths()
│   │
│   ├── _get_file_system_locations_by_type()  # Split LOCAL/SHARED
│   │
│   └── _get_asset_groups()                   # Group by root
│       │
│       ├── [For each input_path]:
│       │   ├── Resolve absolute path
│       │   ├── Skip if relative to SHARED location
│       │   ├── Match to LOCAL location or top directory
│       │   └── Add to group.inputs
│       │
│       ├── [For each output_path]:
│       │   └── Same logic → group.outputs
│       │
│       └── [For each referenced_path]:
│           └── Same logic → group.references
│
└── _get_total_input_size_from_asset_group()
```

---

#### `hash_assets_and_create_manifest(...) -> tuple[SummaryStatistics, list[AssetRootManifest]]`

**Input:**
- `asset_groups`: list[AssetRootGroup]
- `total_input_files`, `total_input_bytes`: Totals
- `hash_cache_dir`: Optional[str]
- `on_preparing_to_submit`: Progress callback

**Output:** Tuple of (stats, list of AssetRootManifest)

**Call Stack:**
```
hash_assets_and_create_manifest()
│
├── ProgressTracker(PREPARING_IN_PROGRESS)
│
└── [For each group]:
    │
    ├── HashCache()                         # Open hash cache
    │
    └── _create_manifest_file()             # Hash and create manifest
        │
        ├── ThreadPoolExecutor()
        │
        └── [For each path]:
            └── _process_input_path()
                │
                ├── Check hash_cache for existing entry
                │
                ├── [If modified or new]:
                │   └── hash_file()         # Compute hash
                │
                └── Create manifest Path object
```

---

#### `upload_assets(...) -> tuple[SummaryStatistics, Attachments]`

**Input:**
- `manifests`: list[AssetRootManifest]
- `on_uploading_assets`: Progress callback
- `s3_check_cache_dir`: Optional[str]
- `manifest_write_dir`: Optional[str]

**Output:** Tuple of (stats, Attachments object for job creation)

**Call Stack:**
```
upload_assets()
│
├── _get_total_input_size_from_manifests()
├── ProgressTracker(UPLOAD_IN_PROGRESS)
│
└── [For each asset_root_manifest]:
    │
    ├── Build ManifestProperties
    │
    └── asset_uploader.upload_assets()      # Upload manifest + files
        │
        ├── Upload manifest to S3
        └── upload_input_files()            # Upload all files
```

---

## Data Structures

### AssetRootGroup
```python
@dataclass
class AssetRootGroup:
    root_path: str = ""
    inputs: set[Path] = field(default_factory=set)
    outputs: set[Path] = field(default_factory=set)
    references: set[Path] = field(default_factory=set)
    file_system_location_name: Optional[str] = None
```

### AssetRootManifest
```python
@dataclass
class AssetRootManifest:
    root_path: str
    asset_manifest: Optional[BaseAssetManifest]
    outputs: list[Path]
    file_system_location_name: Optional[str] = None
```

### AssetUploadGroup
```python
@dataclass
class AssetUploadGroup:
    asset_groups: list[AssetRootGroup]
    total_input_files: int
    total_input_bytes: int
```

### ManifestProperties
```python
@dataclass
class ManifestProperties:
    rootPath: str
    rootPathFormat: PathFormat
    inputManifestPath: Optional[str] = None
    inputManifestHash: Optional[str] = None
    outputRelativeDirectories: Optional[list[str]] = None
    fileSystemLocationName: Optional[str] = None
```

### Attachments
```python
@dataclass
class Attachments:
    manifests: list[ManifestProperties]
    fileSystem: str = "COPIED"  # or "VIRTUAL"
```

---

## S3 Key Structure

### Content-Addressable Storage (CAS)
```
s3://{bucket}/{rootPrefix}/Data/{hash}.{algorithm}
```
Example: `s3://my-bucket/DeadlineCloud/Data/abc123def456.xxh128`

### Manifests
```
s3://{bucket}/{rootPrefix}/Manifests/{farm_id}/{queue_id}/Inputs/{hash}_input
```

### Output Manifests
```
s3://{bucket}/{rootPrefix}/Manifests/{farm_id}/{queue_id}/{job_id}/{step_id}/{task_id}/{timestamp}_{session_action_id}/{hash}_output
```

---

## Edge Case Handling

### Security Edge Cases

#### 1. Symlink Attack Prevention (upload.py)
```python
def _open_non_symlink_file_binary(self, path: str):
    """
    Prevents time-of-check/time-of-use (TOCTOU) vulnerabilities.
    """
    open_flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        open_flags |= os.O_NOFOLLOW  # Don't follow symlinks
    
    fd = os.open(path, open_flags)
    
    # Windows: Verify with GetFinalPathNameByHandleW
    if sys.platform == "win32":
        if not _is_path_win32_final_path_of_file_descriptor(path, fd):
            raise OSError(errno.ELOOP, "Mismatch between path and its final path")
    
    # Verify resolved path matches
    if str(Path(path).resolve()) != path:
        raise OSError(errno.ELOOP, "Mismatch between path and its final path")
```

#### 2. Directory Escape Prevention (asset_sync.py)
```python
def _is_file_within_directory(self, file_path: Path, directory_path: Path) -> bool:
    """
    Prevents files from being uploaded/downloaded outside session directory.
    """
    real_file_path = file_path.resolve()
    real_directory_path = directory_path.resolve()
    common_path = os.path.commonpath([real_file_path, real_directory_path])
    return common_path.startswith(str(real_directory_path))
```

#### 3. Output File Security Check (asset_sync.py)
```python
# In _get_output_files():
file_real_path = file_path.resolve()
is_file_path_under_session_dir = self._is_file_within_directory(
    file_real_path, session_dir
)
if is_file_path_under_session_dir is False:
    self.logger.info(
        f"Skipping file '{file_path}' as its resolved path '{file_real_path}' is"
        f" outside the session directory '{session_dir}'"
    )
    continue
```

---

### S3 Error Handling

#### 1. 404 Not Found with Cache Guidance (asset_sync.py)
```python
except JobAttachmentsS3ClientError as exc:
    if exc.status_code == 404:
        raise JobAttachmentsS3ClientError(
            action=exc.action,
            status_code=exc.status_code,
            bucket_name=exc.bucket_name,
            key_or_prefix=exc.key_or_prefix,
            message=(
                "This can happen if the S3 check cache on the submitting machine is out of date. "
                "Please delete the cache file from the submitting machine, usually located in the "
                "home directory (~/.deadline/cache/s3_check_cache.db) and try submitting again."
            ),
        ) from exc
```

#### 2. KMS Permission Detection (upload.py, download.py)
```python
status_code_guidance = {
    403: (
        "Forbidden or Access denied. Please check your AWS credentials..."
        if "kms:" not in str(exc)
        else (
            "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket "
            "encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or "
            "User has the 'kms:GenerateDataKey' and 'kms:DescribeKey' permissions..."
        )
    ),
}
```

#### 3. S3 Check Cache Integrity Verification (upload.py)
```python
def verify_hash_cache_integrity(self, s3_check_cache_dir, manifest, s3_cas_prefix, s3_bucket):
    """
    Samples up to 30 cached entries and verifies they exist in S3.
    If any are missing, the cache is invalid and should be reset.
    """
    s3_upload_keys = [self._generate_s3_upload_key(file, ...) for file in manifest.paths]
    random.shuffle(s3_upload_keys)
    
    sampled_cache_entries = []
    for upload_key in s3_upload_keys:
        entry = s3_cache.get_connection_entry(...)
        if entry is not None:
            sampled_cache_entries.append(entry)
            if len(sampled_cache_entries) >= 30:
                break
    
    return self._check_hashes_exist_in_s3(sampled_cache_entries)
```

---

### File System Edge Cases

#### 1. Disk Capacity Check (asset_sync.py)
```python
def _ensure_disk_capacity(self, session_dir: Path, total_input_bytes: int):
    disk_free = shutil.disk_usage(session_dir).free
    if total_input_bytes > disk_free:
        raise AssetSyncError(
            f"Total file size required for download ({input_size_readable}) "
            f"is larger than available disk space ({disk_free_readable})"
        )
```

#### 2. Windows Long Path Handling
```python
# Throughout the codebase:
local_manifest_file = _get_long_path_compatible_path(
    Path(manifest_write_dir, manifest_name)
)

# In manifest_group.py:
if sys.platform == "win32" and len(manifest_out.manifest) >= WINDOWS_MAX_PATH_LENGTH:
    if not _is_windows_long_path_registry_enabled():
        # Warn user about potential issues
```

#### 3. Missing Output Directory (asset_sync.py)
```python
# In _get_output_files():
if not output_root.is_dir():
    self.logger.info(f"Found 0 files (Output directory {output_root} does not exist.)")
    continue  # Don't fail - another task might create it
```

#### 4. Empty Input Directory Handling
```python
# Empty directories become references since manifest spec can't represent them
if is_dir_empty:
    logger.info(f"Input directory '{directory}' is empty. Adding to referenced paths.")
    asset_references.referenced_paths.add(directory)
```

---

### Path Mapping Edge Cases

#### 1. Cross-Platform Path Format Conversion (asset_sync.py)
```python
# In _get_output_files():
source_path_format = manifest_properties.rootPathFormat
current_path_format = PathFormat.get_host_path_format()

for output_dir in manifest_properties.outputRelativeDirectories or []:
    if source_path_format != current_path_format:
        if source_path_format == PathFormat.WINDOWS:
            output_dir = output_dir.replace("\\", "/")
        elif source_path_format == PathFormat.POSIX:
            output_dir = output_dir.replace("/", "\\")
```

#### 2. Non-ASCII Path Handling in S3 Metadata
```python
# S3 metadata must be ASCII
metadata = {"Metadata": {"asset-root": json.dumps(root_path, ensure_ascii=True)}}
try:
    root_path.encode(encoding="ascii")
    metadata["Metadata"]["asset-root"] = root_path
except UnicodeEncodeError:
    # Use JSON-encoded version for non-ASCII paths
    metadata["Metadata"]["asset-root-json"] = json.dumps(root_path, ensure_ascii=True)
```

#### 3. Storage Profile Path Mapping Mismatch
```python
if manifest_properties.rootPath in storage_profiles_source_paths:
    local_root = storage_profiles_path_mapping_rules[manifest_properties.rootPath]
else:
    raise AssetSyncError(
        "Error occurred while attempting to sync input files: "
        f"No path mapping rule found for the source path {manifest_properties.rootPath}"
    )
```

---

### Cancellation Handling

#### 1. Upload Cancellation (upload.py)
```python
def handler(bytes_uploaded):
    if progress_tracker:
        should_continue = progress_tracker.track_progress_callback(bytes_uploaded)
        if not should_continue and future is not None:
            future.cancel()

# After upload:
except concurrent.futures.CancelledError as ce:
    if progress_tracker and progress_tracker.continue_reporting is False:
        raise AssetSyncCancelledError("File upload cancelled.", progress_tracker.get_summary_statistics())
```

#### 2. Hash Cancellation (upload.py)
```python
def _process_input_path(self, path, root_path, hash_cache, progress_tracker, update=True):
    if progress_tracker and not progress_tracker.continue_reporting:
        raise AssetSyncCancelledError("File hashing cancelled.", progress_tracker.get_summary_statistics())
```

#### 3. Download Cancellation (download.py)
```python
def handler(bytes_downloaded):
    if progress_tracker:
        should_continue = progress_tracker.track_progress_callback(bytes_downloaded)
        if not should_continue:
            future.cancel()

# After download:
except concurrent.futures.CancelledError as ce:
    if progress_tracker and progress_tracker.continue_reporting is False:
        raise AssetSyncCancelledError("File download cancelled.")
```

---

### VFS (Virtual File System) Edge Cases

#### 1. VFS Fallback to COPIED (asset_sync.py)
```python
try:
    VFSProcessManager.find_vfs()
    mount_vfs_from_manifests(...)
    return True
except VFSExecutableMissingError:
    logger.error(
        f"Virtual File System not found, falling back to {JobAttachmentsFileSystem.COPIED}"
    )
    return False
```

#### 2. VFS Requirements Check
```python
# VFS only supported on non-Windows with specific conditions:
if (
    attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value
    and sys.platform != "win32"
    and fs_permission_settings is not None
    and os_env_vars is not None
    and "AWS_PROFILE" in os_env_vars
    and isinstance(fs_permission_settings, PosixFileSystemPermissionSettings)
):
    # Use VFS
else:
    # Fall back to COPIED
```

---

### File Conflict Resolution (download.py)

#### CREATE_COPY with Thread Safety
```python
def _get_new_copy_file_path(local_file_name, collision_lock, collision_file_dict):
    with collision_lock:
        file_str = str(local_file_name)
        num = collision_file_dict[file_str]
        new_file_name = local_file_name
        
        while True:
            try:
                # Atomic file creation to handle multi-process conflicts
                with open(new_file_name, "x"):
                    break
            except FileExistsError:
                num += 1
                new_file_name = local_file_name.parent.joinpath(
                    f"{local_file_name.stem} ({num}){local_file_name.suffix}"
                )
        
        collision_file_dict[file_str] = num
        return new_file_name
```

---

### Modification Time Tracking (asset_sync.py)

#### Microsecond Precision
```python
# Record mtime in nanoseconds (from stat)
self.synced_assets_mtime[abs_path] = Path(abs_path).stat().st_mtime_ns

# In _get_output_files():
mtime_when_synced = self.synced_assets_mtime.get(str(file_path), None)
file_mtime = file_path.stat().st_mtime_ns

if mtime_when_synced:
    if file_mtime > int(mtime_when_synced):
        is_modified = True  # File changed during session
else:
    # New file created during session
    self.synced_assets_mtime[str(file_path)] = int(file_mtime)
    is_modified = True
```

---

### Large File Handling (upload.py)

#### Serial Upload for Large Files
```python
# Rationale: Better to use multi-threaded multipart upload for one large file
# than multiple large files at the same time (wastes less bandwidth on cancel)

(small_file_queue, large_file_queue) = self._separate_files_by_size(
    manifest.paths, self.small_file_threshold
)

# Small files: parallel upload
with ThreadPoolExecutor(max_workers=self.num_upload_workers) as executor:
    futures = {executor.submit(upload_object_to_cas, file, ...): file for file in small_file_queue}

# Large files: serial upload (but still parallel multipart internally)
for file in large_file_queue:
    upload_object_to_cas(file, ...)
```
