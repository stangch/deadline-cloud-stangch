# Job Attachments Manifest Format Update - Summary

## Overview

This document proposes updates to the Deadline Cloud Job Attachments manifest format to address gaps discovered through practical use. The goal is to make local rendering and cloud rendering produce equivalent results.

## Key Changes

### New Requirements
- Support for **snapshot manifests** (full directory tree) and **diff manifests** (changes only)
- Empty directory representation
- File and directory deletion markers
- POSIX execute bit support
- Optional symlink support (relative, within directory tree only)
- Large file chunking (256MB chunks)
- Scale validation: 1M+ files/directories

### Manifest Format

The new format uses compressed directory name storage with indexed references:

```json
{
    "hashAlg": "xxh128",
    "manifestVersion": "2022-06-06",
    "dirs": [
        { "name": "env_sandbox" },
        { "name": "$0/RaceCarToy" },
        { "name": "$0/render_output" },
        { "name": "$1/textures" }
    ],
    "files": [
        {
            "name": "$1/RaceCarToy.blend",
            "hash": "198bf506ca3c16dd8411e1c9fd197252",
            "size": 12345,
            "mtime": 1234567890
        },
        {
            "name": "$3/RaceCarToy_BaseColor.png",
            "hash": "661e12aa2b9095fc34d5b2a0008d8319",
            "size": 12345,
            "mtime": 1234567890
        }
    ]
}
```

### Directory Indexing
- `$N` references the Nth directory in the `dirs` array (zero-indexed)
- Directories reference their parent using the same `$N` syntax
- Root directory files have no prefix

### Large File Chunking (>256MB)
```json
{
    "name": "$1/large_file.blend",
    "chunkhashes": [
        "198bf506ca3c16dd8411e1c9fd197252",
        "3e99e2c87affe2f2133e73805636d57d",
        "4eeaff8007d87e83cc550dfddca55c08"
    ],
    "size": 1539294804,
    "mtime": 1234567890
}
```

### Diff Manifest Features
- `parentManifestHash` field references the base snapshot
- File deletion: `{ "name": "$3/file.txt", "delete": true }`
- Directory deletion: `{ "name": "$3/dir_to_delete", "delete": true }`

### Content-Type Headers
- Snapshot: `application/x-deadline-manifest-YYYY-MM-DD`
- Diff: `application/x-deadline-manifest-diff-YYYY-MM-DD`

### Optional Fields
- `runnable: true` - POSIX execute bit
- `symlink: { "name": "$2/target.blend" }` - symlink support

## Workflow

1. Job submission creates a **base snapshot manifest**
2. Task outputs create **diff manifests**
3. Output manifest aggregation combines diffs with defined layering order
4. Workers download base + applicable diffs
5. Artists download outputs by applying diffs locally

## Benefits
- Resumable uploads for large files
- Efficient partial file modifications (only changed chunks re-uploaded)
- Explicit diff semantics fix current step-to-step data flow bugs
- Canonical format enables manifest hashing for deduplication