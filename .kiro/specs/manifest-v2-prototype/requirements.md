# Requirements Document

## Introduction

This spec defines a lightweight prototype implementation of the Job Attachments Manifest V2 format. The prototype will demonstrate the core concepts of the V2 format including directory indexing, file chunking for large files, and a download implementation that reconstructs files from chunked storage.

## Glossary

- **Manifest V2**: The proposed next-generation manifest format with directory compression, chunking support, and snapshot/diff capabilities
- **Content-Addressable Storage (CAS)**: S3 storage where files are named by their content hash
- **Chunk**: A segment of a large file (>256MB), stored separately in CAS. All chunks are 256MB except the final chunk which may be smaller
- **CRT**: AWS Common Runtime - a high-performance C library for AWS SDK operations
- **Directory Index**: Zero-based integer reference ($N) pointing to entries in the dirs array
- **Snapshot Manifest**: A complete representation of a directory tree at a point in time
- **Download Function**: Function that reconstructs files from manifest and CAS chunks

## Requirements

### Requirement 1

**User Story:** As a developer, I want to define the Manifest V2 data structure, so that I can represent directory trees with the new compressed format.

#### Acceptance Criteria

1. WHEN the system creates a Manifest V2 structure THEN the system SHALL include fields for manifestVersion, hashAlg, dirs array, and files array
2. WHEN a directory is represented THEN the system SHALL store it with a name field using $N syntax for parent references
3. WHEN a file is represented THEN the system SHALL include name, size, and mtime fields
4. WHEN a file is less than or equal to 256MB THEN the system SHALL include a single hash field
5. WHEN a file is greater than 256MB THEN the system SHALL include a chunkhashes array instead of a hash field

### Requirement 2

**User Story:** As a developer, I want to create sample manifest files with nested directories, so that I can test the V2 format with realistic data.

#### Acceptance Criteria

1. WHEN sample manifests are created THEN the system SHALL generate at least two example files in JSON format
2. WHEN a sample includes nested directories THEN the system SHALL use the $N directory indexing syntax correctly
3. WHEN a sample includes small files THEN the system SHALL represent them with a single hash field
4. WHEN a sample includes large files THEN the system SHALL represent them with a chunkhashes array
5. WHEN a sample includes an empty directory THEN the system SHALL include it in the dirs array with no files referencing it

### Requirement 3

**User Story:** As a developer, I want to implement a download function for V2 manifests, so that I can reconstruct files from chunked CAS storage.

#### Acceptance Criteria

1. WHEN the download function receives a manifest and S3 options THEN the system SHALL parse the V2 manifest structure
2. WHEN downloading a small file THEN the system SHALL fetch the single chunk from CAS using the hash
3. WHEN downloading a large file THEN the system SHALL fetch all chunks in order and concatenate them, where all chunks are 256MB except the final chunk which may be smaller
4. WHEN reconstructing a file THEN the system SHALL create the directory structure using the dirs array
5. WHEN all files are downloaded THEN the system SHALL set the modification time to the mtime value from the manifest

### Requirement 4

**User Story:** As a developer, I want the download implementation to be separate from existing code, so that it doesn't interfere with current functionality.

#### Acceptance Criteria

1. WHEN the download code is implemented THEN the system SHALL place it in a new file separate from existing modules
2. WHEN the download function is called THEN the system SHALL NOT modify or import existing upload/download modules
3. WHEN the implementation is complete THEN the system SHALL provide either a standalone function or CLI command interface
4. WHEN errors occur THEN the system SHALL provide clear error messages without affecting existing systems

### Requirement 5

**User Story:** As a developer, I want to use boto3 with CRT for S3 operations, so that I can achieve optimal download performance.

#### Acceptance Criteria

1. WHEN the download function initializes S3 client THEN the system SHALL use boto3 with CRT if available
2. WHEN CRT is not available THEN the system SHALL fall back to standard boto3 S3 client
3. WHEN downloading chunks THEN the system SHALL leverage CRT's automatic multipart download and parallelism
4. WHEN downloading chunks THEN the system SHALL benefit from CRT's automatic retries at the C layer

### Requirement 6

**User Story:** As a developer, I want to write tests for the download functionality, so that I can verify correct file reconstruction.

#### Acceptance Criteria

1. WHEN tests are executed THEN the system SHALL verify small file download and reconstruction
2. WHEN tests are executed THEN the system SHALL verify large file chunked download and reconstruction with variable-sized final chunk
3. WHEN tests are executed THEN the system SHALL verify directory structure creation from the dirs array
4. WHEN tests are executed THEN the system SHALL verify modification time is set correctly
5. WHEN tests are executed THEN the system SHALL verify the download handles missing chunks with appropriate errors
