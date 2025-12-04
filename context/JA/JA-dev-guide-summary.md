# Job Attachments Developer Guide - Summary

## Overview
Job Attachments is a Deadline Cloud feature (GA April 2024) that uses S3 as content-addressable storage (CAS) to efficiently manage job assets. Files are hashed and stored by hash value, enabling deduplication - identical files across jobs are only uploaded once.

## Core Concept: Asset Manifest
JSON schema defining file snapshots with:
- `manifestVersion`: Schema version (2023-03-03)
- `hashAlg`: Hashing algorithm (xxh128)
- `paths[]`: File entries with hash, mtime, path, size
- `totalSize`: Total bytes

## Lifecycle (4 Phases)

### 1. Job Submission
- Job Bundle provides Asset References listing input files
- Files are hashed and uploaded to S3 CAS
- Input Asset Manifests created and associated with job

### 2. Worker Session Start (Input Sync)
- Worker recreates file system snapshot from Input Manifests
- Downloads assets to `/sessions/OpenJD/{session}/assetroot-{hash}/`
- Original folder structure replicated on worker

### 3. Worker Session Complete (Output Sync)
- Output folders recursively listed
- Files hashed and uploaded to S3 Data folder
- Output Manifests created with job/step/task/session hierarchy

### 4. Output Download
- CLI: `deadline job download-output`
- Lists Output Manifests, downloads files to workstation
- Only most recent output downloaded for retried tasks

## S3 Data Layout
```
s3://bucket/prefix/
  -> Data/           # Content-addressed files ({hash}.xxh128)
  -> Manifests/      # Organized by farm/queue/job/step/task
       -> Inputs/    # Input manifests ({hash}_input)
       -> Outputs/   # Output manifests ({hash}_output)
```

## Step-Step Dependencies
- Steps can depend on outputs of prior steps
- Worker lists dependent step's Output Manifests from S3
- Downloads required CAS files before execution
- Job-to-Job dependencies NOT currently supported

## Key Code Locations
- Upload: `deadline/job_attachments/upload.py`
- Download: `deadline/job_attachments/download.py`
- Asset Sync: `deadline/job_attachments/asset_sync.py`
- Job Bundle Submit: `deadline/client/api/_submit_job_bundle.py`

## Future Work
- Manifest format updates
- Output Manifest aggregation
- Checkpointing
- VFS performance optimization
- Job-Job dependencies
- Filtered output transfers
