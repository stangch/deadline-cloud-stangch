# Manifest V2 Prototype Design

## Overview

This design describes a lightweight prototype implementation of the Job Attachments Manifest V2 format. The prototype demonstrates the core V2 concepts: directory compression via indexing, large file chunking (256MB chunks with variable-sized final chunk), and a download function that reconstructs files from Content-Addressable Storage (CAS).

The implementation is intentionally isolated from existing production code to allow rapid prototyping without risk to current functionality.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Manifest V2 Prototype                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────┐      ┌──────────────────┐            │
│  │  Data Structures │      │  Sample Files    │            │
│  │                  │      │                  │            │
│  │  - ManifestV2    │      │  - small.json    │            │
│  │  - DirEntry      │      │  - large.json    │            │
│  │  - FileEntry     │      │                  │            │
│  │  - ChunkInfo     │      │                  │            │
│  └──────────────────┘      └──────────────────┘            │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Download Function                          │   │
│  │                                                       │   │
│  │  1. Parse V2 Manifest                               │   │
│  │  2. Create Directory Structure                       │   │
│  │  3. Download Files (small: 1 chunk, large: N chunks)│   │
│  │  4. Set File Metadata (mtime)                       │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                    │
│                          ▼                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           S3 Client (boto3 + CRT)                    │   │
│  │                                                       │   │
│  │  - CRT if available (high performance)              │   │
│  │  - Standard boto3 fallback                          │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                    │
└──────────────────────────┼────────────────────────────────────┘
                           ▼
                  ┌─────────────────┐
                  │   S3 CAS        │
                  │                 │
                  │  Data/          │
                  │    {hash}.xxh128│
                  └─────────────────┘
```

## Components and Interfaces

### 1. Data Structures

#### ManifestV2
```python
@dataclass
class ManifestV2:
    manifestVersion: str  # "2024-12-04" or similar
    hashAlg: str          # "xxh128"
    dirs: list[DirEntry]  # Sorted list of directories
    files: list[FileEntry] # List of files
```

#### DirEntry
```python
@dataclass
class DirEntry:
    name: str  # Either "dirname" or "$N/dirname" where N is parent index
```

#### FileEntry (Small File)
```python
@dataclass
class FileEntry:
    name: str      # "$N/filename" where N is directory index, or "filename" for root
    hash: str      # xxh128 hash for files <= 256MB
    size: int      # File size in bytes
    mtime: int     # Unix timestamp (seconds)
```

#### FileEntry (Large File)
```python
@dataclass
class FileEntry:
    name: str                # "$N/filename"
    chunkhashes: list[str]   # List of chunk hashes for files > 256MB
    size: int                # Total file size in bytes
    mtime: int               # Unix timestamp (seconds)
```

#### ChunkInfo (Internal)
```python
@dataclass
class ChunkInfo:
    hash: str      # Chunk hash
    offset: int    # Byte offset in file
    size: int      # Chunk size (256MB except last chunk)
```

### 2. Download Function

```python
def download_v2_manifest(
    manifest_path: str,
    s3_bucket: str,
    cas_prefix: str,
    download_dir: str,
    session: Optional[boto3.Session] = None
) -> DownloadResult:
    """
    Download files from S3 CAS using a V2 manifest.
    
    Args:
        manifest_path: Path to V2 manifest JSON file
        s3_bucket: S3 bucket name
        cas_prefix: CAS prefix (e.g., "Data")
        download_dir: Local directory to download files to
        session: Optional boto3 session (will use CRT if available)
    
    Returns:
        DownloadResult with statistics
    """
```

#### DownloadResult
```python
@dataclass
class DownloadResult:
    files_downloaded: int
    bytes_downloaded: int
    directories_created: int
    errors: list[str]
```

### 3. S3 Client Initialization

```python
def get_s3_client(session: Optional[boto3.Session] = None):
    """
    Get S3 client with CRT if available, otherwise standard boto3.
    
    CRT provides:
    - Automatic multipart download
    - Parallel transfers
    - Automatic retries at C layer
    - Memory-efficient streaming
    """
    session = session or boto3.Session()
    
    # Check if CRT is available
    try:
        import awscrt
        # CRT is available, boto3 will use it automatically
        return session.client('s3')
    except ImportError:
        # Fall back to standard boto3
        return session.client('s3')
```

## Data Models

### Manifest V2 JSON Structure

#### Small File Example
```json
{
    "manifestVersion": "2024-12-04",
    "hashAlg": "xxh128",
    "dirs": [
        {"name": "assets"},
        {"name": "$0/textures"}
    ],
    "files": [
        {
            "name": "$1/small_texture.png",
            "hash": "abc123def456",
            "size": 1048576,
            "mtime": 1733270400
        }
    ]
}
```

**Explanation**: 
- `dirs[0]` = "assets" (root directory)
- `dirs[1]` = "$0/textures" (textures inside assets, which is index 0)
- File "$1/small_texture.png" means small_texture.png is in directory index 1 (assets/textures)

#### Large File Example
```json
{
    "manifestVersion": "2024-12-04",
    "hashAlg": "xxh128",
    "dirs": [
        {"name": "renders"}
    ],
    "files": [
        {
            "name": "$0/large_render.exr",
            "chunkhashes": [
                "chunk1hash",
                "chunk2hash",
                "chunk3hash"
            ],
            "size": 600000000,
            "mtime": 1733270400
        }
    ]
}
```

**Explanation**:
- `dirs[0]` = "renders" (root directory)
- File "$0/large_render.exr" means large_render.exr is in directory index 0 (renders)
- File has 3 chunks because size > 256MB

### Directory Indexing Rules

1. Root-level directories have `name` without prefix: `"env_sandbox"`
2. Nested directories reference parent with `$N/dirname`: `"$0/RaceCarToy"` (where 0 is index in dirs array, meaning RaceCarToy is inside directory 0)
3. Files reference their directory with `$N/filename`: `"$1/file.blend"` (where 1 is index in dirs array)
4. Root-level files have no prefix: `"readme.txt"`
5. The dirs array is lexicographically sorted for canonical representation

### Chunking Rules

1. Files ≤ 256MB: Single `hash` field
2. Files > 256MB: `chunkhashes` array
3. Chunk size: 256MB (268435456 bytes) for all chunks except the last
4. Last chunk: Remainder bytes (can be any size from 1 byte to 256MB)
5. S3 key format: `{cas_prefix}/{chunk_hash}.xxh128`

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Directory references are valid
*For any* manifest with directory entries, all $N references in directory names and file names must point to valid indices in the dirs array
**Validates: Requirements 1.2**

### Property 2: File entries have required fields
*For any* file entry in a manifest, it must contain name, size, and mtime fields
**Validates: Requirements 1.3**

### Property 3: Small files use hash field
*For any* file entry where size ≤ 268435456 bytes, the entry must have a hash field and must not have a chunkhashes field
**Validates: Requirements 1.4**

### Property 4: Large files use chunkhashes array
*For any* file entry where size > 268435456 bytes, the entry must have a chunkhashes array and must not have a hash field
**Validates: Requirements 1.5**

### Property 5: Manifest parsing succeeds
*For any* valid V2 manifest JSON, the download function must successfully parse it without errors
**Validates: Requirements 3.1**

### Property 6: Small file download fetches correct chunk
*For any* small file in a manifest, downloading it must fetch exactly one chunk from S3 using the key `{cas_prefix}/{hash}.xxh128`
**Validates: Requirements 3.2**

### Property 7: Large file download fetches all chunks
*For any* large file in a manifest, downloading it must fetch all chunks in the chunkhashes array in order and concatenate them to produce a file of the expected size
**Validates: Requirements 3.3**

### Property 8: Directory structure is created
*For any* manifest with directories, downloading must create all directories listed in the dirs array before downloading files
**Validates: Requirements 3.4**

### Property 9: File modification times are preserved
*For any* downloaded file, its modification time must match the mtime value from the manifest entry
**Validates: Requirements 3.5**

### Property 10: Missing chunks raise errors
*For any* file download where a chunk is missing from S3, the download function must raise a clear error indicating which chunk is missing
**Validates: Requirements 4.4**

## Error Handling

### Error Types

1. **ManifestParseError**: Invalid JSON or missing required fields
2. **InvalidReferenceError**: $N reference points to non-existent directory
3. **ChunkNotFoundError**: Chunk hash not found in S3 CAS
4. **ChunkSizeMismatchError**: Downloaded chunk size doesn't match expected
5. **S3AccessError**: Permission or connectivity issues with S3

### Error Handling Strategy

```python
class ManifestV2Error(Exception):
    """Base exception for Manifest V2 operations"""
    pass

class ManifestParseError(ManifestV2Error):
    """Raised when manifest JSON is invalid"""
    pass

class InvalidReferenceError(ManifestV2Error):
    """Raised when $N reference is invalid"""
    pass

class ChunkNotFoundError(ManifestV2Error):
    """Raised when chunk is missing from S3"""
    def __init__(self, chunk_hash: str, s3_key: str):
        self.chunk_hash = chunk_hash
        self.s3_key = s3_key
        super().__init__(f"Chunk not found: {s3_key}")
```

## Testing Strategy

### Unit Tests

Unit tests will cover specific examples and edge cases:

1. **Manifest parsing**: Parse valid and invalid manifests
2. **Directory resolution**: Resolve $N references to actual paths
3. **S3 key construction**: Verify correct S3 keys for chunks
4. **Error conditions**: Missing chunks, invalid references

### Property-Based Tests

Property-based tests will verify universal properties across many inputs:

1. **Property 1-4**: Manifest structure validation
2. **Property 5-10**: Download behavior verification

Each property test will:
- Generate random manifests with various structures
- Execute the download function (with mocked S3)
- Verify the property holds

### Test Configuration

- Property tests: Minimum 100 iterations per property
- Test framework: pytest with hypothesis for property-based testing
- Mocking: Use moto for S3 mocking or custom mock objects

### Test Data

Sample manifests will be created in `test/data/manifest_v2/`:
- `small_files.json`: Manifest with only small files
- `large_files.json`: Manifest with chunked files
- `nested_dirs.json`: Manifest with deep directory nesting
- `empty_dirs.json`: Manifest with empty directories
- `mixed.json`: Manifest with combination of all features

## Implementation Notes

### File Location

New file: `src/deadline/job_attachments/manifest_v2_prototype.py`

This keeps the prototype isolated from existing code in:
- `src/deadline/job_attachments/upload.py`
- `src/deadline/job_attachments/download.py`
- `src/deadline/job_attachments/asset_sync.py`

### CLI Command (Optional)

If CLI interface is desired:
```python
# In src/deadline/client/cli/_groups/manifest_group.py

@cli_manifest.command(name="download-v2")
@click.argument("manifest_file")
@click.option("--bucket", required=True)
@click.option("--cas-prefix", default="Data")
@click.option("--download-dir", required=True)
def manifest_download_v2(manifest_file, bucket, cas_prefix, download_dir):
    """Download files using a V2 manifest (PROTOTYPE)"""
    result = download_v2_manifest(
        manifest_path=manifest_file,
        s3_bucket=bucket,
        cas_prefix=cas_prefix,
        download_dir=download_dir
    )
    click.echo(f"Downloaded {result.files_downloaded} files ({result.bytes_downloaded} bytes)")
```

### CRT Integration

The boto3 client will automatically use CRT if the `awscrt` package is installed. No special configuration needed beyond:

```python
# CRT is used automatically if installed
s3_client = session.client('s3')
```

Benefits when CRT is available:
- Automatic multipart downloads for large chunks
- Parallel transfer of multiple chunks
- Automatic retries at C layer (more efficient)
- Memory-efficient streaming

### Chunk Size Constant

```python
CHUNK_SIZE = 256 * 1024 * 1024  # 268435456 bytes = 256MB
```

### Download Algorithm

```python
def download_file_from_manifest(file_entry: FileEntry, ...):
    """Download a single file from manifest"""
    
    # Determine if small or large file
    if hasattr(file_entry, 'hash'):
        # Small file: single chunk
        chunk_data = download_chunk(file_entry.hash, ...)
        write_file(chunk_data, ...)
    else:
        # Large file: multiple chunks
        with open(local_path, 'wb') as f:
            for chunk_hash in file_entry.chunkhashes:
                chunk_data = download_chunk(chunk_hash, ...)
                f.write(chunk_data)
    
    # Set mtime
    os.utime(local_path, (file_entry.mtime, file_entry.mtime))
```

## Sample Files

Two sample manifest files will be created:

### 1. `samples/manifest_v2_small.json`
- 3-4 directories with nesting
- 5-6 small files (< 256MB)
- 1 empty directory
- Demonstrates basic V2 structure

### 2. `samples/manifest_v2_large.json`
- 2-3 directories
- 2 small files
- 2 large files with chunking (3-4 chunks each)
- Demonstrates chunking with variable-sized final chunks

## Future Enhancements (Out of Scope)

This prototype intentionally omits:
- Diff manifests (only snapshot manifests)
- File deletion markers
- Directory deletion markers
- Symlink support
- POSIX execute bit (runnable field)
- Content-Type headers in S3
- Progress callbacks
- Parallel file downloads
- Resume capability
- Integration with existing upload.py

These features can be added in future iterations once the core V2 format is validated.
