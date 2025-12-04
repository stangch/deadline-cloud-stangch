# Job Attachments Developer Guide
TLDR;
"Day in the life of Job Attachments, and its role in the Job lifecycle" - What Job Attachment does at Deadline Cloud GA April, 2024.

---
## README
Read the public Job Submission User guide prior to going into the details of this guide. The public user guide provides a good end to end view of how things works. This guide is a deep dive into how the code flow works.

https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/getting-started.html
---
## 1.0 What / Why Job Attachments?
Job attachments uses Deadline Cloud's Queue configured S3 bucket as a [content-addressable storage][1], which creates a snapshot of the files used in your job submission in [asset manifests][2], only uploading files that aren't already in S3. This saves you time and bandwidth when iterating on jobs. Only modified files will be uploaded to S3. Content Addressable Storage is modelled by hashing the contents of a file, and storing the file named as the hash value. Files containing the same binary content will result in the same hash, thus saving asset upload time on Job Submission. For example, a character model may be shared between Shots 1 and 2. When Shot 1 is submitted, the character model is hashed to the value `123abc` and uploaded to S3. When Shot 2 is submitted, Deadline's Job Attachments feature will detect the character model hash `123abc` is already on S3 to save upload time and bandwidth.

The core concept of Job Attachments is the Asset Manifest schema hosted on [Github][3]. Take the following example.

```
{  
    "manifestVersion": "2023-03-03",  
    "hashAlg": "xxh128",  
    "paths": [  
        {  
            "hash": "20a443a514c8325c162ff3c5ca1d3161",  
            "mtime": 1712972834000000,  
            "path": "assets/input.png",  
            "size": 5356783  
        }  
    ],  
    "totalSize": 5356783  
}
```
Every Asset Manifest contains the following attributes:
* `manifestVersion: "2023-03-03"`, Schema version designed to be extensible in the future.
* `hashAlg: "xx128"`, Hashing algorithm used to compute file hashes. At launch, only [xxh128][4] hashing is supported.
* `path: []` , A list of objects describing a content addressable file. Each file has the following properties.
  * `hash`: The file hash computed using `hashAlg` . This value is used as the storage file name.
  * `mtime`: The file time stamp of the file.
  * `path:` The local file path, and file name of a file.
  * `size:` The file size, in bytes.
* `totalSize: number`, the total number of bytes for all files associated with the manifest.

Developer Referenced information:
* [Understanding Storage for AWS Deadline Cloud][5]
* [README.md][6]
* [Job Attachment Asset Manifest][7]

## 2.0 What is the Lifecycle of Job Attachments in Job Execution?
Job Attachments is involved in four phases of Job Submission, Execution to Downloading Outputs. In the context of this section, use of Storage Profiles is omitted for simplification. All assets are LOCAL to the job submission and data is exchanged using S3.

1. When a user submits a Job, either from DCC or CLI.
* When a Job submission process is started, a [Job Bundle][8] is provided to Deadline Cloud's Client side tooling. A Bundle's [Asset References][9] list the set of input files, and file paths required for job execution. Deadline cloud's client software hashes each file as a content addressable element and uploads the data to S3. Input **Asset Manifests** are created, uploaded to S3 and associated with a job submission.
2. When a Worker Session is started, assets are required to render a particular Job-Session.
* When an [AWS Deadline Cloud worker agent][10] starts working on a job-session with job attachments, it recreates the file system snapshot of step 1 in the worker agent session directory using the job's Input **Asset Manifests**. Job Attachments downloads all inputs and structures the asset files exactly the same as local rendering.
3. When a Worker Session is completed, outputs are uploaded back to S3
* When the Worker Agent completes a job-session, output folders annotated in [Asset References][9] are recursively listed, and uploaded back to the S3 Content Addressable Storage. The content addressed output files of each Job-Session-Task is associated with an Output Manifest file.
4. When the user downloads the output of a Job from DCC or CLI.
* When the user wants to view the results of rendering a Job, the deadline client retrieves all output manifests of a Job, Step or Task and downloads each output file from the Content Addressable storage back to the local workstation.

Developer Referenced information:
* Mark's white board diagram.

## 3.0 Job Attachments at Job creation
### 3.2 From Job Bundles
When a Deadline Cloud Queue is created, `jobAttachmentSettings` ([boto3][12]) is a required property of the Create Queue request. When a Job is submitted, a Queue's configured `jobAttachmentSettings` provides the `s3BucketName` and `rootPrefix` where Job Attachment files are uploaded. For each asset file in a bundle's [assetsReferences][13], the file is hashed, uploaded to S3. Input Asset Manifest(s) are created for the Job Submission, uploaded to S3 and included as part of the [create_job][14] request.

`Todo: Need to add more "how" many manifests are created. This is only the simple case.`
### 3.3 What is the data layout on S3?

```
s3://my-storage-bucket  
 -> PrefixFolder  
    -> Data  
      -> 20a443a514c8325c162ff3c5ca1d3161.xxh128  
      -> aed61cd2df405c0e4fa3d56a07fd890a.xxh128  
      -> 1ca097621ec5c100569ff7f53e64031c.xxh128  
      -> ...  
    -> Manifests  
      -> farm-00000000000000000000000000000000  
        -> queue-11111111111111111111111111111111  
          -> Inputs/ {GUID} # GUID here needs to be removed. It serves no purpose.  
            -> 12345678123456781234567812345678_input  
            -> abcdef01abcdef01abcdef01abcdef01_input
```
Lets dive into the actual layout of the Content Address storage on Job submissions. In this example, the `s3BucketName` is configured to `s3://my-storage-bucket`, and `rootPrefix` is configured to `PrefixFolder`. The root path of this Content Addressable store is `s3://my-storage-bucket/PrefixFolder`. Within this root, there are two folders:
* `Data`, this is where all the data files are stored. Note in this example there are 3 files with a 32 character hash as name, and extension `xxh128` (Hashing algorithm).
* `Manifests` , this folder is nested and organized in the form `/{farm-id}/{queue-id}/Inputs/` . All Input Asset Manifest from a Job, Queue are stored in this folder. Input Manifests are named with a 32 character hash concatenated with `_input`. Notice this folder structure containing Farm and Queue ID. This folder structure is designed to allow sharing of the S3 Content Address Storage across Queues and Farms.

```
# Contents of 12345678123456781234567812345678_input  
{  
    "hashAlg": "xxh128",  
    "manifestVersion": "2023-03-03",  
    "paths": [  
        {  
            "hash": "20a443a514c8325c162ff3c5ca1d3161",  
            "mtime": 1712972834000000,  
            "path": "assets/MP4/Trailer1080.mp4",  
            "size": 5356783  
        },  
        {  
            "hash": "aed61cd2df405c0e4fa3d56a07fd890a",  
            "mtime": 1712967372000000,  
            "path": "assets/SRC/watermark.png",  
            "size": 26205  
        },  
        {  
            "hash": "1ca097621ec5c100569ff7f53e64031c",  
            "mtime": 1712977780000000,  
            "path": "assets/SRC/Balrog.nk",  
            "size": 7104  
        }  
    ],  
    "totalSize": 5390092  
}
```
The Input Manifest utilize the Asset Manifest specification to enumerate input files. As an example, the asset manifest `12345678123456781234567812345678_input` models a job with 3 input files, totalling 5390092 bytes.

For example, file `assets/MP4/Trailer1080.mp4` hashes to `20a443a514c8325c162ff3c5ca1d3161` . In the content addressable store, notice how `20a443a514c8325c162ff3c5ca1d3161.xxh128` exists in the `Data` directory. The input manifest path object provides the mapping between input asset file-path, and the associated hash named file in the Content Addressable Data folder. Two other input files are presented for reference.

**To add**: How does this work with multiple asset roots.

Reference Material:
* Refer to assetsReferences.yaml [README.md][13]
* Create Job from Bundle ([code][15])
  * Process inputs by asset groups ([code][16])
  * Input Manifest - creation ([code][17]),
    * Hashes each file and creates a manifest ([code][18])
    * Uploads manifest, where file name is the hash of the input ([code][19])
    * Uploads all assets, where file name is the hash of the file content (code)
      * If a CAS file already exists, skip it. ([code][20])
    * Constructs the Attachments structure, so the input can be passed to `create_job`
      * From Job API, Attachments structure ([code][21]), [boto3][14]

## 4.0 Job Attachments During Session Execution
Once a job is submitted to Deadline Cloud, workers are assigned sessions representing Tasks within a single Step of a Job. Each Session is comprised of Input Asset synchronization, Conda Environment Bootstrapping [Optional], one or more task runs and finally Output synchronization and cleanup. In this section, each step involved with Job Attachments will be explored.
### 4.1 Input Asset Synchronization

```
# On the worker:  
/sessions/OpenJD/{session temp dir}  
  -> /assetroot-{hash}/  
    -> assets/MP4/Trailer1080.mp4 (20a443a514c8325c162ff3c5ca1d3161.xxh128)  
    -> assets/SRC/watermark.png.  (aed61cd2df405c0e4fa3d56a07fd890a.xxh128)  
    -> assets/SRC/Balrog.nk       (1ca097621ec5c100569ff7f53e64031c.xxh128)  
  -> /assetroot-{hash2}/  
    -> files/abc.png  
    -> files/123.exr
```
Input Asset Synchronization is the first step of a session. In this session action, a Job's inputs are copied from the Content Address Storage to the worker. Continuing from the example in the prior section, the Input manifest consists of 3 files. On the worker, each asset manifest's file is copied to a unique folder named `assetroot-{hash}`. In the example, content file `20a443a514c8325c162ff3c5ca1d3161.xx128hash` is downloaded to the asset folder with the original path `/assets/MP4/Trailer1080.mp4`. Notice how the original job submission folder structure is replicated to the worker.

Developer Reference Material
* Worker Agent downloads the manifest, and downloads all assets.
* [Code][22], [Code root][23]
* [Windows long path limitations / support][24]
### 4.2 Worker Job Attachments Output 
A) Worker Output

```
/sessions/OpenJD/{session temp dir}/asset-root-{hash}/  
  -> output/output_0001.png
```
B) Output Manifest

```
# abcdef01abcdef01abcdef01abcdef01_output  
{  
    "hashAlg": "xxh128",  
    "manifestVersion": "2023-03-03",  
    "paths": [  
        {  
            "hash": "623c01f620299050f3fd828bbd0cac9e",  
            "mtime": 1712283778226271,  
            "path": "output/output_0001.png",  
            "size": 7986453  
        }  
    ],  
    "totalSize": 7986453  
}
```
C) S3 Data Layout

```
S3://my-storage-bucket  
 -> Prefix Folder  
    -> Data  
      -> 623c01f620299050f3fd828bbd0cac9e.xxh128  
    -> Manifests  
      -> farm-00000000000000000000000000000000  
        -> queue-11111111111111111111111111111111  
          -> job-2222222222222222222222222222222222  
            -> step-333333333333333333333333333333333  
              -> task-444444444444444444444444444444444  
                -> {time}-session-action-55555555555555555555555555555555-1  
                   -> abcdef01abcdef01abcdef01abcdef01_output
```
In the last step of session execution on a worker, outputs of the session are saved back to the Content Address Storage. Job Attachments will recursively list and find all files stored in outputs paths defined by `dataFlow` `OUT,INOUT` of the job template and outputs defined by a bundle's `assetReferences`. Similar to Input files, Job Attachments is used to store output files as content addressed storage. For example, the example explored thus far had and output file `/output/output_0001.png` , located in the file path as depicted in A) above. Job Attachments will first hash the file `output_0001.png`, computing `623c01f620299050f3fd828bbd0cac9e` as the hash. The output file is then uploaded to S3 in the Data directory, with file name  `623c01f620299050f3fd828bbd0cac9e.xxh128` . A corresponding Output Asset Manifest is generated to model the output files and hash relationship (B). The Output Manifest is uploaded to `Manifests` folder but stored nested under a folder structure representing the Session's job lineage. The full file structure representing the output file and manifest are presented in C).

**TODO**: Relationship of the bundle assetReference to dataFlow IN-OUT.
**Developer internal note:** on Queue Group permissions to session temp dir access that are being fixed: [Asset Syncing as Job User][25]

Reference Material
* IN-OUT dataflows on job submission: [https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/client/job_bundle/parameters.py#L620-L626][26]
* Sync Inputs and step-step inputs ([code][27])
* Output Manifest ([code][28])
* Looking for Output files - GetOutputFiles ([Code][29]), uploadOutputManifestToS3 ([Code][30]), output manifest full path ([Code][31]), join of all job, step, task, session-action-id
* Sync Output ([code][32])
  * What are the output files. ([code][33])
  * Output comes from submitted job (Smithy [model][34])(client [model][35])

### 4.3 Job Attachments for step-step dependencies
A) Example Step-Step template:

```
name: Step-Step Dependency Test  
specificationVersion: 'jobtemplate-2023-09'  
steps:  
- name: A  
    ....  
- name: B  
  dependencies:  
  - dependsOn: A # This means Step B depends on Step A  
     ....  
- name: C  
  dependencies: # This means Step B depends on Step A and Step B  
  - dependsOn: A  
  - dependsOn: B   
     ....   
```
B) Step - Step dependency storage reference

```
S3://my-storage-bucket  
 -> Prefix Folder  
    -> Data  
      -> aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xxh128  
      -> bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.xxh128  
    -> Manifests  
      -> farm-00000000000000000000000000000000  
        -> queue-11111111111111111111111111111111  
          -> job-2222222222222222222222222222222222  
            -> step-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  
              -> task-444444444444444444444444444444444  
                -> {time}-session-action-55555555555555555555555555555555-1  
                   -> abcdef01abcdef01abcdef01abcdef01_output  
            -> step-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB  
              -> task-777777777777777777777777777777777  
                -> {time}-session-action-88888888888888888888888888888888-1  
                   -> qwerty01qwerty01qwerty01qwerty01_output
```
Deadline Cloud supports [Step-Step Dependencies][36], where steps within a Job can be chained as dependencies to form an execution Control Flow Graph. Lets take an example provided by the Job template in A). Step A has no dependencies and is executed first. Step B depends on Step A's output. Step C depends on both Step A and B's output. From the prior section, outputs of each step are stored to S3 and referenced via an Output Manifest. For readability, the has file output of Steps A and B are named "`aaaa...xxhash`" and "`bbb....xxhash`" When the worker executes Step B, the output manifests of Step A are listed from S3 then all content addressed data are downloaded to the session directory. All outputs of Step A are downloaded by performing a S3 list object with matching `s3://my-storage-bucket/PrefixFolder/Manifest/farm-000.../queue-111..../job-2222/step-AAA.../*/*_output`. In this example, the output CAS file `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xxh128` is downloaded. Similarly, when Step C is executed, the output manifests for Step A and Step B are similarly listed. Files `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xxh128` and `bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.xxh128` are downloaded for session execution.

Job-to-Job dependencies is currently not supported for Deadline Cloud.

Developer Reference Material
* [Code][37], pulls in output manifest from prior step
* [Code][38] to do a listObjectsV2 with prefix, and _output filter, picking up only the last rendered session action output.
  * Most important method is `get_output_manifests_by_asset_root`, shared with download as well. Integration points ([link][39])
## 5.0 Job Attachments Output Download 
5.1 Downloading job output back to workstation

```
S3://my-storage-bucket  
 -> Prefix Folder  
    -> Data  
      -> 623c01f620299050f3fd828bbd0cac9e.xxh128  
    -> Manifests  
      -> farm-00000000000000000000000000000000  
        -> queue-11111111111111111111111111111111  
          -> job-2222222222222222222222222222222222  
            -> step-333333333333333333333333333333333  
              -> task-444444444444444444444444444444444  
                -> {time}-session-action-55555555555555555555555555555555-1  
                   -> abcdef01abcdef01abcdef01abcdef01_output  
                -> {time2}-session-action-55555555555555555555555555555555-1  
                   -> qwerty01qwerty01qwerty01qwerty01_output
```
Finally, after a Job is successfully completed on Deadline Cloud, users will want to download the output back to their workstation for review. Job Attachments offers a CLI command (`deadline job download-output`) to download outputs at Job, Step or Task level. The CLI command first lists all Output Manifest objects ending with `_output`, located under the S3 folder structure prefixed at job, step, task IDs. Next, all output files contained in output manifests are downloaded back to the user's workstation and remapped via the output folder structure. Note; for any tasks that are executed multiple times from retries, or manual re-execution, the Job Attachments download feature will filter and download only the most recent output. This is illustrated in the example above. If a task is executed twice, where `time2` > `time.` Only the output from `time2` will be downloaded.

The Deadline Cloud Monitor also provides an easy to use Download [feature][40] to easily execute the download CLI command for Job, Step or Task.

Developer Reference Material:
* [Download.py][41]
* Deadline Cloud job download CLI - [Code][42] see here what options are available.

---
## 6.0 Other features
### Job Attachments snapshotting
* [Sean TDD Job Attachment CLI Commands][43] (Intern project) Currently on a branch. This feature will allow customers to snapshot and "pre-cache" assets up to a JA bucket. Users will save time from clicking submit until job is created. The CLI tool also has diff commands to visually see previously uploaded versions and latest on disk differences.
---
## The Future
Job Attachments is not finished! There are still ongoing projects the team should consider and keep in mind for any designs.
* [Deadline Cloud Job Attachments Manifest Format Update for 2024][44]
* [Output Manifest Aggregation][45]
* [BeaLine Checkpointing TDD][46]
* Performance + Benchmarking w/ VFS end to end. Remember Mark's diagram on the board? How can we test every aspect of a Job's assets and outputs.
* Fixing boto3 CRT [COE]
* When to use VFS and when to use regular uploads. What is the efficiency cross over if there is one?
* Job-Job dependencies when we support it? How do we sync the right outputs from one job to the next, similar to step-step dependency.
* Filtering output and/or step-step dependency upload and download. Users may only want some of the files to be transferred. (Idea from Daniel, eg saving .tx files to split maya + arnold to separate steps)
---
5.2 Job output to Job input dependencies. (Out of Scope)
* [Job-job Dependencies TDD][47]
* Do we pull or have an output manifest at a job? No → Manifest aggregation [Output Manifest Aggregation][45]
  * Need to implement job-job dependencies for attachments
---

[1]: https://en.wikipedia.org/wiki/Content-addressable_storage
[2]: https://github.com/aws-deadline/deadline-cloud/tree/mainline/src/deadline/job_attachments#asset-manifests
[3]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_manifests/schemas/2023-03-03.json
[4]: https://xxhash.com/
[5]: https://quip-amazon.com/Op79AdRhCGcd
[6]: https://github.com/aws-deadline/deadline-cloud/tree/mainline/src/deadline/job_attachments
[7]: https://quip-amazon.com/oKBTAPmH1aFq
[8]: https://github.com/aws-deadline/deadline-cloud-samples/tree/mainline/job_bundles
[9]: https://github.com/aws-deadline/deadline-cloud-samples/tree/mainline/job_bundles#elements---asset-references
[10]: https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/
[12]: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline/client/create_queue.html
[13]: https://github.com/aws-deadline/deadline-cloud-samples/blob/mainline/job_bundles/README.md#elements---asset-references
[14]: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline/client/create_job.html
[15]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/client/api/_submit_job_bundle.py#L49
[16]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/upload.py#L1092
[17]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/client/api/_submit_job_bundle.py#L419
[18]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/upload.py#L717
[19]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/upload.py#L139
[20]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/upload.py#L345
[21]: https://code.amazon.com/packages/AmazonBeaLineModel/blobs/a58ca806c5d83f2712dafe46f818db2d67ae87a3/--/model/resources/jobs/jobs.types.smithy#L115
[22]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/download.py#L236
[23]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/download.py#L281
[24]: https://quip-amazon.com/JunTATtqabtQ
[25]: https://quip-amazon.com/kRqDA9K3RGGE
[26]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/client/job_bundle/parameters.py#L620-L626
[27]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/asset_sync.py#L354
[28]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L595
[29]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L198
[30]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L146
[31]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/asset_sync.py#L599C50-L599C68
[32]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L536
[33]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L588
[34]: https://code.amazon.com/packages/AmazonBeaLineModel/blobs/a58ca806c5d83f2712dafe46f818db2d67ae87a3/--/model/resources/jobs/jobs.types.smithy#L82
[35]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/models.py#L161
[36]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/jobs-scheduling.html#jobs-scheduling-dependencies
[37]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/asset_sync.py#L447
[38]: https://github.com/aws-deadline/deadline-cloud/blob/74293acadd15d5e9437f94d8a8e8b8d908a84d2c/src/deadline/job_attachments/download.py#L166
[39]: https://github.com/search?q=repo%3Aaws-deadline%2Fdeadline-cloud%20get_output_manifests_by_asset_root&type=code
[40]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/download-finished-output.html
[41]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/job_attachments/download.py#L698
[42]: https://github.com/aws-deadline/deadline-cloud/blob/mainline/src/deadline/client/cli/_groups/job_group.py#L634
[43]: https://quip-amazon.com/KcxnAQErvjev
[44]: https://quip-amazon.com/acBCA1Fu0lAy
[45]: https://quip-amazon.com/im2RAj0rWXPe
[46]: https://quip-amazon.com/STunAesJt9JH
[47]: https://quip-amazon.com/d8mZAWsoWcjG
