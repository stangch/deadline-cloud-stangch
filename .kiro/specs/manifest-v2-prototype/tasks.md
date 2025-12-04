# Implementation Plan

- [ ] 1. Create data structures for Manifest V2
  - Create Python dataclasses for ManifestV2, DirEntry, FileEntry in new file
  - Implement logic to distinguish small files (hash field) vs large files (chunkhashes field)
  - Add validation for required fields (manifestVersion, hashAlg, dirs, files)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ]* 1.1 Write property test for manifest structure validation
  - **Property 1: Directory references are valid**
  - **Validates: Requirements 1.2**

- [ ]* 1.2 Write property test for file field requirements
  - **Property 2: File entries have required fields**
  - **Validates: Requirements 1.3**

- [ ]* 1.3 Write property test for small file hash field
  - **Property 3: Small files use hash field**
  - **Validates: Requirements 1.4**

- [ ]* 1.4 Write property test for large file chunkhashes field
  - **Property 4: Large files use chunkhashes array**
  - **Validates: Requirements 1.5**

- [ ] 2. Create sample manifest files
  - Create `samples/manifest_v2_small.json` with nested directories and small files
  - Create `samples/manifest_v2_large.json` with chunked files and variable-sized final chunks
  - Include at least one empty directory in samples
  - Validate samples parse correctly with data structures
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ] 3. Implement S3 client initialization with CRT support
  - Create function to initialize boto3 S3 client
  - Add CRT detection (try import awscrt)
  - Implement fallback to standard boto3 if CRT unavailable
  - _Requirements: 5.1, 5.2_

- [ ] 4. Implement manifest parsing function
  - Create function to load and parse V2 manifest JSON
  - Validate manifest structure and required fields
  - Raise ManifestParseError for invalid manifests
  - _Requirements: 3.1_

- [ ]* 4.1 Write property test for manifest parsing
  - **Property 5: Manifest parsing succeeds**
  - **Validates: Requirements 3.1**

- [ ] 5. Implement directory resolution logic
  - Create function to resolve $N references to actual directory paths
  - Build directory tree from dirs array
  - Validate all $N references point to valid indices
  - Raise InvalidReferenceError for invalid references
  - _Requirements: 3.4_

- [ ]* 5.1 Write property test for directory structure creation
  - **Property 8: Directory structure is created**
  - **Validates: Requirements 3.4**

- [ ] 6. Implement chunk download function
  - Create function to download single chunk from S3 CAS
  - Construct S3 key: `{cas_prefix}/{chunk_hash}.xxh128`
  - Handle ChunkNotFoundError for missing chunks
  - Return chunk bytes
  - _Requirements: 3.2, 3.3_

- [ ]* 6.1 Write property test for small file download
  - **Property 6: Small file download fetches correct chunk**
  - **Validates: Requirements 3.2**

- [ ]* 6.2 Write property test for large file download
  - **Property 7: Large file download fetches all chunks**
  - **Validates: Requirements 3.3**

- [ ]* 6.3 Write property test for missing chunk error handling
  - **Property 10: Missing chunks raise errors**
  - **Validates: Requirements 4.4**

- [ ] 7. Implement file download and reconstruction
  - Create function to download file from manifest entry
  - For small files: download single chunk
  - For large files: download all chunks in order and concatenate
  - Write reconstructed file to local path
  - Handle variable-sized final chunks correctly
  - _Requirements: 3.2, 3.3_

- [ ] 8. Implement file metadata setting
  - Set file modification time (mtime) after download
  - Use os.utime() to set both access and modification times
  - _Requirements: 3.5_

- [ ]* 8.1 Write property test for mtime preservation
  - **Property 9: File modification times are preserved**
  - **Validates: Requirements 3.5**

- [ ] 9. Implement main download function
  - Create download_v2_manifest() function with signature from design
  - Parse manifest
  - Create all directories from dirs array
  - Download all files
  - Return DownloadResult with statistics
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 4.3_

- [ ] 10. Add error handling and custom exceptions
  - Define ManifestV2Error base exception
  - Define ManifestParseError, InvalidReferenceError, ChunkNotFoundError
  - Add clear error messages with context (chunk hash, S3 key, etc.)
  - _Requirements: 4.4_

- [ ] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ]* 12. Write integration test for end-to-end download
  - Test complete download flow with mocked S3
  - Verify files are reconstructed correctly
  - Verify directory structure matches manifest
  - Test with both sample manifests
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ]* 13. Add CLI command (optional)
  - Add download-v2 command to manifest_group.py
  - Wire up to download_v2_manifest function
  - Add click options for bucket, cas-prefix, download-dir
  - _Requirements: 4.3_
