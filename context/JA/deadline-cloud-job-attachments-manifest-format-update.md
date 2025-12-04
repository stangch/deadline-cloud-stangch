# Deadline Cloud Job Attachments Manifest Format Update

## 1. Introduction

Practical use of job attachments has revealed gaps/areas for improvement in the system, some of which require a new revision of the manifest file format. This doc summarizes what we have found so far.

**NOTE:** This is a starting proposal for the next manifest format update. We will likely find additional improvements to make before we ship that update, and should incorporate those improvements into this doc!

## 2. References

* [Job Attachment Asset Manifest](https://quip-amazon.com/oKBTAPmH1aFq) - first manifest format specification
* [One-Pager: BeaLine Job Attachments Bucket Structure](https://quip-amazon.com/aqFKAXhqPwaY)
* [github job_attachments/asset_manifests/v2023_03_03](https://github.com/aws-deadline/deadline-cloud/tree/mainline/src/deadline/job_attachments/asset_manifests/v2023_03_03) - implementation of the 2023-03-03 format
* [Output Manifest Aggregation](https://quip-amazon.com/im2RAj0rWXPe)

## 3. Job attachments north star

* Our **litmus test for job attachments features** is to make the following experiences equivalent, except where we make explicit choices not to:
  * Local render button vs submit job button. The output image file is the same image, has the same filename, and appears in the same local output path.
    * Within a DCC, hitting the "render" button that writes an image to a file.
    * Within a DCC, hitting the "submit to Deadline Cloud" button, letting it run, then downloading the output from Deadline Cloud.
  * Submitting a job to a farm with shared file systems vs job attachments. The output files are the same either way, and are placed in the same shared file system location.
    * Submitting a job bundle to a shared file system farm. It runs the steps and tasks in dependency order. Downstream steps see and use files from their dependencies.
    * Submitting a job bundle to a job attachments farm, then downloading the output. The steps and tasks run identically, but job attachments provides the file system views.
* We have made the following **explicit choices that fail the above litmus test**.
  * Job attachments uses a data flow consistency model, not a POSIX shared file system consistency model. If two tasks are not connected by a dependency chain, neither one can see the output of the other.
  * Job attachments are only propagated along an explicit step-step dependency edge. If two tasks are connected by transitive dependency edge, the downstream step does not see the upstream step's output.
    * **TODO**: I think we should revisit this default, we have found that this choice results in confusing errors for users.
  * We do not preserve POSIX permissions, Windows ACLs, etc.


## 4. Requirements

### 4.1 Previous requirements

* Minimal overhead. I.e., The # of bytes in addition to relative file paths and hashes is small.
* Fast parsing.
* The Asset Manifest is a canonical representation of a directory tree.
  * Ie. the manifest file is hashable and two manifest files that describe the same directory tree would have the same hash.
* Support S3 Select.
  * As outlined in 7.1.3 Example MHL File with Lots of Files and Directories, it is much quicker to use S3 select when only a subset of the manifest file is needed.

### 4.2 Proposed requirements

* **(Same)** Minimal overhead. I.e., The # of bytes in addition to relative file paths and hashes is small.
* **(Same)** Fast parsing.
* **(Same)** The Asset Manifest is a canonical representation of a directory tree.
  * Ie. the manifest file is hashable and two manifest files that describe the same directory tree would have the same hash.
* **(Removed as we never used this)** Support S3 Select.
* **(New)** It can represent either a directory snapshot or a directory difference. (The current form uses the directory snapshot as if it were a difference.)
  * A snapshot represents a full directory tree and the files it contains.
  * A diff represents a set of changes to a directory tree. This includes the files and directories created, modified, and removed. It should likely reference the manifest it was created from (i.e. its hash, since the format is canonical).
* **(New)** It must be able to represent empty directories. The diff form must be able to represent directory deletion.
* **(New)** The diff form must be able to represent file deletion.
* **(New)** It must be able to represent the POSIX "execute" bit. This is likely the way git does it, as a single boolean attribute, not a full POSIX permissions model.
  * **(New)** Maybe it should be able to represent symlinks. A constraint on symlinks would include: Only relative symlinks that point within the directory tree, they are not permitted to escape.
* **(New)** We should decide on a versioned Content-Type that is required to be set when a manifest is in S3.
* **(New)** Any proposed feature of the manifest format must be validated at large scale.
  * Manifests with more than 1 million files.
  * Manifests with more than 1 million directories.
  * Manifests with more than 1 million files in a single directory.
* **(New)** Working with large files (e.g. 65GB or 0.5TB files) must be reliable. If the upload of such a large file to S3 is interrupted, restarting must resume the upload, not start from scratch. If a workload modifies a few bytes in the file, it should be only necessary to process a small portion of the file, not to hash and re-upload the entire thing.
  * The standard approach for this is to split large files into chunks. In order to retain the canonical representation property of this schema, we must specify deterministic semantics of chunking, likely a fixed chunk size and that any file above that size is always chunked.
  * Based on intuition, I propose 256MB chunks. A 0.5TB file becomes 2000 chunks with this size, and one chunk would take 2.2 seconds to transfer at 1Gbps.
  * We can improve efficiency and correctness of data upload. Currently the implementation does two steps: read file + hash, then read file + upload. The user could modify the file between those actions, resulting in data not matching the hash. For large datasets, this double-read is also a performance problem. With large file chunking we can unconditionally do a read + hash + upload, never going back to read the data from disk again.


## 5. Proposed Solution

### 5.1 Format Changes

#### 5.1.1 Compress directory name storage

In test cases where jobs are processing a large volume of files, we have run into bottlenecks. We see this at 150K files, and need to plan for future growth, so have proposed 1 million files and/or directories as a benchmark scale.

If we remove the requirement to support S3 Select, as in the proposed requirements, we have freedom to store two lists, a directory list and a file list. To be canonical, the directory list would be a lexicographically sorted list of all directory names that exist in the snapshot. The root directory of the manifest is not represented. Each name is either the filename or directory name directly, if the file/directory is in the root of the manifest. Otherwise, it is in the form "$N/<name>" where N is a zero-based integer index into the "dirs" list.

Here's how the example looks with this change, once again pretty printed instead of canonical. Note that the directory `render_output` has no files in it, this shows how the manifest can represent empty directories that exist. (Comments are not part of the JSON, they're to help connect the directory indexes.)

```json
{
    "hashAlg": "xxh128",
    "manifestVersion": "2022-06-06",
    "dirs": [{
        # "$0" "env_sandbox"
        "name": "env_sandbox"
    }, {
        # "$1" "RaceCarToy"
        "name": "$0/RaceCarToy"
    }, {
        # "$2" "env_sandbox/render_output"
        "name": "$0/render_output"
    }, {
        # "$3" "RaceCarToy/textures"
        "name": "$1/textures"
    }],
    "files": [{
        # "RaceCarToy/RaceCarToy.blend"
        "name": "$1/RaceCarToy.blend",
        "hash": "198bf506ca3c16dd8411e1c9fd197252",
        "size": 12345,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/RaceCarToy.blend1"
        "name": "$1/RaceCarToy.blend1",
        "hash": "f46fc657ddfe322ce8fdf0af867371b9",
        "size": 12345,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/textures/RaceCarToy_BaseColor.png"
        "name": "$3/RaceCarToy_BaseColor.png",
        "hash": "661e12aa2b9095fc34d5b2a0008d8319",
        "size": 12345,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/textures/RaceCarToy_Normal.png"
        "name": "$3/RaceCarToy_Normal.png",
        "hash": "fa03f3f6fac3bbe30c3832212405187d",
        "size": 12345,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/textures/RaceCarToy_Roughness.png"
        "name": "$3/RaceCarToy_Roughness.png",
        "hash": "23e260bc7e14b9e86424d80f6c53ad74",
    }, {
        # "RaceCarToy/textures/rooitou_park_2k.hdr"
        "name": "$3/rooitou_park_2k.hdr",
        "hash": "ed9490838b143ccb9787d9320174a1ef",
        "size": 12345,
        "mtime": 1234567890
    }, {
        # "env_sandbox/env_sandbox.blend"
        "name": "$0/env_sandbox.blend",
        "hash": "e96eff658d34aada345aaf8fdfc4981a",
        "size": 12345,
        "mtime": 1234567890
    }]
}
```

#### 5.1.2 Method to distinguish snapshot manifest vs diff manifest

To distinguish between a snapshot manifest and a diff manifest, we propose the following:
* These formats have different Content-Type stored in S3 metadata. See 5.1.7 Manifest Content-Type below.
* A snapshot manifest never has a "parentManifestHash" field, but a diff manifest always has it.
  * The "parentManifestHash" contains the hash (using the hash algorithm recorded in the manifest) of the canonical form of the snapshot manifest that the diff was created from. Note, it is required to be a snapshot manifest, it cannot be another diff manifest.
  * **PROBLEM**: What if someone wants to create a diff manifest that is abstract, that doesn't have a starting snapshot? We should likely do something different than the current proposal here that accommodates that.
* A snapshot manifest cannot contain any of the following:
  * File or directory deletion.

###
# 5.1.3 Files greater than 256MB are chunked

When a file is greater than 256MB (256 * 2**20 = 268435456 bytes), it is always divided into chunks, where each chunk is exactly 256MB in size except possibly the final one. In this case, there is no "hash" field, instead there is a "chunkhashes" field containing a list of all the hashes of the individual chunks. This is mandatory to follow this precisely, so that the manifest is canonical.

This feature will improve the behavior around modifying part of a very large file. If a process opens a file like this to write, and either modifies a part of it or appends to it, most of the chunks will stay exactly the same, and only one or two chunks will have a different hash value. The chunks that stay the same do not need re-uploading due to the content-addressed storage, and if we implement support for this in the Deadline VFS, the unchanged chunks also don't need to be re-hashed because the VFS knows precisely which bytes were modified.

```json
{
    "hashAlg": "xxh128",
    "manifestVersion": "2022-06-06",
    "dirs": [{
        # "$0" "env_sandbox"
        "name": "env_sandbox"
    }, {
        # "$1" "RaceCarToy"
        "name": "$0/RaceCarToy"
    }, {
        # "$2" "RaceCarToy/textures"
        "name": "$1/textures"
    }],
    "files": [{
        # "RaceCarToy/RaceCarToy.blend"
        "name": "$1/RaceCarToy.blend",
        "chunkhashes": [
            "198bf506ca3c16dd8411e1c9fd197252",
            "3e99e2c87affe2f2133e73805636d57d",
            "4eeaff8007d87e83cc550dfddca55c08",
            "a4f492b49defe2076f5c50df715b5e18",
            "ef2a9347dc90994bc523f92cf818b74e",
            "437d82007c58d9e19c2b2817f96e38bf"
        ],
        "size": 1539294804,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/RaceCarToy.blend1"
        "name": "$1/RaceCarToy.blend1",
        "chunkhashes": [
            "198bf506ca3c16dd8411e1c9fd197252",
            "aa11005fd6f5cbc503a4d8e9a7e131de",
            "b938b3e9ac543810b288a9aae10c3e2d"
        ],
        "size": 733988436,
        "mtime": 1234567890
    }, {
        # "RaceCarToy/textures/RaceCarToy_BaseColor.png"
        "name": "$2/RaceCarToy_BaseColor.png",
        "hash": "661e12aa2b9095fc34d5b2a0008d8319",
        "size": 12345,
        "mtime": 1234567890
    }]
}
```

#### 5.1.4 File deletion marker

To indicate a file deletion, a file entry has fields `dir`, `name`, and `delete: true`. This is only permitted in diff manifests, not in snapshot manifests.

TODO: Probably change this to a top-level list of deleted files like `"deletedFiles": [{"name": "$3/file_to_delete.txt"}, ...]`.

```json
{
    "name": "$3/blender_file_that_is_being_deleted.blend",
    "delete": true
}
```

#### 5.1.5 Directory deletion marker

Directory deletions are indicated in the `dirs` list. This is only permitted in diff manifests, not in snapshot manifests.

TODO: Probably change this to a top-level list of deleted files like `"deletedDirs": [{"name": "$3/dir_to_delete"}, ...]`.

There is an ambiguity about deleting files or other directories within a directory that has a deletion marker. We resolve it, in order to make things canonical as follows: If a particular directory has a delete marker, no directories in the `dirs` list and no files in the `files` list are permitted to reference it (including within any `symlink` sub-object).

```json
{
    "name": "$3/directory_to_delete",
    "delete": true
}
```

#### 5.1.6 POSIX execute bit

To store whether a file has the POSIX execute bit set, we propose to match the `runnable` field specified in the Open Job Description Schema for the <EmbeddedTextFile> object. To make it canonical, we propose:
* If a file is not executable, the `runnable` field is excluded. A field `runnable: false` never appears in a valid manifest.
* When a file is executable, the field `runnable: true` appears after its `mtime`.

```json
{
    "name": "$1/attached_script_file.sh",
    "hash": "198bf506ca3c16dd8411e1c9fd197252",
    "size": 12345,
    "mtime": 1234567890,
    "runnable": true
}
```

#### 5.1.7 Symlinks

If we choose to support symlinks in the manifest format, I propose we use a second `name` to store their target. This form encodes that a symlink target is always within the manifest directory. One difference from POSIX is that this format can only store symlinks pointing to a directory that actually exists, while POSIX symlinks can point into non-existent directories. The symlink would also have no `hash` field.

```json
{
    "name": "$1/RaceCarToy.blend",
    "symlink": {
        "name": "$2/RaceCarToy_v208.blend"
    }
}
```

#### 5.1.8 Manifest Content-Type

While the distinguishing feature within a manifest is whether it contains the field `parentManifestHash` or not, we propose that in S3 storage we require that the `Content-Type` metadata is set, and that it be set to one of the following two values:
* For snapshot manifest: `Content-Type: application/x-deadline-manifest-YYYY-MM-DD`
* For diff manifest: `Content-Type: application/x-deadline-manifest-diff-YYYY-MM-DD`

At a future date when we add a similar manifest format to OpenJD, we can use new names prefixed with `openjd` instead of `deadline`.

---


## Comments

### Comment Thread 1

> Deadline Cloud Job Attachments Manifest Format Update

David Leong: 😅 I think this will be JA Manifest Format 2025 at the rate we're going

Mark Wiebe: Maybe the first line of code can still happen in 2024 for it?

### Comment Thread 2

> TODO: I think we should revisit this default, we have found that this choice results in confusing errors for users. One possible moment to apply such a change is when we ship Output Manifest Aggregation, so that the behavior is different when a customer opts in to the service-side manifest aggregation.

Caden Marofke: +1

### Comment Thread 3

> (New) It can represent either a directory snapshot or a directory difference. (The current form uses the directory snapshot as if it were a difference.)

Caden Marofke: So to be abundantly clear, when we submit a job we create a 'full directory tree' (i.e. 'base' manifest). And the output manifests from tasks become 'diff manifests', correct? So then the output manifest aggregation component is just combining diffs, and deciding layering (perhaps from the job template as you mentioned previously).

So when a worker starts a session, it downloads the 'base' manifest, then any combined diffs if there are step deps, and creates a new diff manifest when it's done.

When an artist downloads outputs then, we just apply the manifest diff locally, which would just download any new files, and/or delete files/directories, leaving their local filesystem state the same as the last step that completed.

(Just trying to think through the entire user flow with the new manifest format)

Definitely some threats to jot down around deleting files when downloading outputs, especially when we roll out an auto-download solution.

Mark Wiebe: Yes, that's right! The characteristics of deleting are similar to overwriting files, so we should likely group that into the analysis we already did for overwriting.

Caden Marofke: Fair point!

Beej Nodora: For my understanding, what's the advantage of manifest representing a diff?

It sounds like this would reduce processing time as we're only parsing and applying the diff.

Does it introduce equivalent processing time to generate the diff?

Mark Wiebe: We're currently using the non-diff manifest as if it were a diff in the implementation. This makes the diff explicit so it's less confusing and fixes bugs we have in the current step to step data flow.

### Comment Thread 4

> Based on intuition, I propose 256MB chunks. A 0.5TB file becomes 2000 chunks with this size, and one chunk would take 2.2 seconds to transfer at 1Gbps.

David Leong: Was thinking about this a little, should have have a "chunk size" property per file so we can easily offset into the file for byte-range reads from VFS?

This can also be adjustable later by DCC / Workload.

EG: if we notice Maya reads the first 100 meg file for some header info then seeks 1gb at a time somewhere else in the file. I'm making an assumption here binary files "usually" would start with some header section followed by binary dumps of data structures.

Mark Wiebe: If we do that, then the format would not be canonical.

For reads, you can always use smaller chunks and ranged reads within a single manifest-level chunk.

David Leong: True, VFS would have to do smaller range reads of the first file (or first few files) and then download larger blocks after.

### Comment Thread 5

> {    "hashAlg": "xxh128",    "manifestVersion": "2022-06-06", ...}

Caden Marofke: Love this

Nit: for completeness, we could include an unused directory to illustrate the requirement of allowing empty dirs

Mark Wiebe: Great idea, I've added it.

Cecilia Cho: from this example, the structure would be like the picture I captured? (Quip doesn't support indentation nicely)

However, I think if there are many directories/files, it might be little confusing in the files array. For example, It's hard to identify what is the parent directory for `env_sandbox.blend`

How about this? https://paste.amazon.com/show/chocecil/1715963934

Mark Wiebe: There's always a tradeoff between good machine-processable data formats, and human readability. I think we need to go for the former, and write high quality tooling to compensate for the latter.

Thanks for the idea, we should explore many potential different format schemas! The flat arrays seem better and simpler for automated processing to me, compared to adding a nested structure within the format.

Cecilia Cho: yeah understand the trade offs.

The reason i was confused about the structure, is the dir values. It seems to be little hard to find parents.

Mark Wiebe: What makes it hard to find the parents? Each path from a subdir to the root is basically a linked list.

Graeme McHale: If space reduction is a key goal, it seems to me you could optimize further by removing the `"dir": 1` entry from the file entry and use something like `"path": "$1/file1.txt"` instead. It reminds me of how json schemas let you define define reusable types.

Mark Wiebe: That sounds like a good idea to me! I'll change it to that.

It's applied.

### Comment Thread 6

> 5.1.3 Files greater than 256MB are chunked

David Leong: One idea while reading the S3 docs on performance. Can we leverage prefixes + file chunking to achieve higher throughput? According to the S3 doc, there's a scaling factor applied to each prefix that will scale out. This implies organizing the CAS data folder with additional prefixes so data files will can have more capacity per overall. This can help especially if we have many small files that needs to be read on demand

https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

Mark Wiebe: I think the prefixes are already there? The '/' directory markers aren't special in any way so taking a few characters into the name is a prefix for S3's purpose, right?

David Leong: Yeah, I was thinking how we organize data in the CAS & manifest folders and manfiests

Today we have:

s3://bucket-name/prefix/data/1234567890.xxhash

s3://bucket-name/prefix/data/abcdefghi.xxhash

s3://bucket-name/prefix/data/qwertyuiop.xxhash

s3://bucket-name/prefix/data/zxcvbnm,.xxhash

What if it was:

s3://bucket-name/prefix/data/1234/567890.xxhash

s3://bucket-name/prefix/data/abcd/efghi.xxhash

s3://bucket-name/prefix/data/qwer/tyuiop.xxhash

s3://bucket-name/prefix/data/zxcv/bnm,.xxhash

Would it yield higher scaling given the statement: "For example, your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix"

Was thinking about the "per partitioned S3 prefix" but you are right, the prefix "/" is just a a character with no meaning.

Maybe a question how S3 partitions and scales given the number of objects

Just randomly searching found this interesting example: https://w.amazon.com/bin/view/CatalogDataWorks/development/BestdesignpracticesforchoosingS3prefixes/

I guess thinking about this we do O(1) direct access to the /Data subfolder, while in manifests we access it by the partitioned farm / queue / job / step / task. The later yields nicely to the prefix scaling

Mark Wiebe: I think actually the former is better because it creates a very even key space distribution.

I did some investigations along these lines prior to the development of job attachments. You can read my findings here: https://quip.com/Op79AdRhCGcd#temp:C:GDMfb59958d63c7077b79ea51dbd

### Comment Thread 7

> If we choose to support symlinks in the manifest format, I propose we use a second name to store their target.

Caden Marofke: Just have to decide how we handle this if the symlink name collides with an existing file

Mark Wiebe: I think it would be the same as if a file name collides with an existing file - we overwrite it. That's a good point to extend also to directories vs files. If a diff manifest deletes a directory and creates a file of the same name, how should we store and process that?

Caden Marofke: Right yea makes sense. I think I have to digest this a little more, and maybe I missed it, but how do we handle collisions if Step C depends on Step A and B, but A and B don't depend on each other? Is that a valid case? Do we pick the last completed one, or choose some order? Or just advise against it?

Mark Wiebe: That's 100% a valid case! We'll need to define a canonical layering order in the https://quip.com/im2RAj0rWXPe design. This applies also to merging all the output manifests of a step's tasks into a single manifest for the step since the independent tasks could write the same file too. Likely the layering order is in the task parameter iteration order defined by the template for that case.

### Other Comments

Caden Marofke: We historically changed the bucket structure to put manifests under `farmid/queueid/` (see https://quip.com/im2RAj0rWXPe#temp:C:cHB0de4dbccd81b45d6b165037ae). This was to address some AWS Portal feedback that customers had about not knowing where things were in their bucket. Ideally the manifesets would be under the JobID as well, but we haven't implemented https://quip.com/eqlsASEPeqFn, so this isn't as helpful as I wanted it to be originally.

Your suggestion seems like would require we move the manifests back to a top-level prefix (i.e. rootPrefix/manifests) so we can quickly reference them. That could be acceptable though, we already rely on the job.attachments.inputManifestPath field in the Job Model to find the S3 key, so it's an easy enough swap. Some tradeoffs to consider here regardless though.

Mark Wiebe: Yeah, I was thinking about these factors when writing it. For our current use cases, I don't think we would need to change anything about our current manifest storage, but we would need to re-consider that for potential applications of it.

If we were to use a location under the `Manifests` prefix, the manifest format would no longer be canonical with respect to the data, it would become dependent on the specific infrastructure it is created within as well.

---

Caden Marofke: That's an interesting requirement I didn't consider

---

Caden Marofke: This is a very interesting idea. I like the idea of symlinks being explicitly modeled in the manifest regardless

Mark Wiebe: At the same time, it's a nerve-wracking topic based on our experience analyzing and mitigating security threats around it!

Caden Marofke: Definitely a big can of worms we'd be opening if we decided to go with this haha

---

Graeme McHale: This would be of interest to a [Berryessa] like product. The use case is:

1. A project owner defines the basic structure of an asset on creation, including the folders it may / must contain.

2. The contractor persona actually doing the 3D scan uploads the scan files into the required asset folders.

The [Berryessa] prototype may well fake this capability by placing hidden files into folders that would be otherwise empty.

---

Graeme McHale: It would take up considerably less space, and be more natural for an UI / API focussed on version diffing, to have a separate list of deleted files. Did you consider that?

Mark Wiebe: That makes sense, yeah. I guess we'd have two top-level lists like "deleteDirs" and "deleteFiles"

---

Graeme McHale: I love this, heavy +1. For [Berryessa] use cases in particular this would be wonderful - particularly:

Contractor starts upload on very low bandwidth mobile connection in the field, and finishes it later on an office LAN.

Along similar lines, we should consider integrity checks too. For example, ensuring that the hash when the object reaches S3 really does match the hash we expected client-side. NSFT had similar functionality.

Mark Wiebe: Awesome. When developing Sequoia (2015-16), one of my main test datasets was a 0.5TB LIDAR point cloud from an aerial survey I got from opentopography. That's roughly my mental model here.

---

Beej Nodora: When a process opens a file, how do we know which chunk it needs?

Mark Wiebe: We don't, we know that when it makes the write call.

David Leong: This also fits into accelerating what files are new / modified when syncing outputs. Today we do recursive list, file size and time checks. But with VFS, we can easily export deltas directly.

---

Stephen Crowe: What about gzipping the manifest? It would lower the complexity of the spec and may perform better since it'll also compress some of the JSON bits.

Mark Wiebe: I hope you mean using zstd and not actually gzip :) (Because gzip is painfully slow)

How would it lower the complexity of the spec?

Stephen Crowe: Sorry forgot to connect the thoughts. Zipping the contents could get the space savings without having to introduce a substitution syntax.

Mark Wiebe: Right, I did consider that and concluded that's not better. I never wrote that thought down, though, gotta keep these docs shorter.

Basically, I think it's better to have a relatively simple deduplication in the data structure compared to having redundancy and then relying on generic compression code to eliminate it. The code does not feel more complicated in a meaningful way. Unlike in OpenJD templates, it's reasonable to reduce the human-readability of the format.