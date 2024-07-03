# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline asset` commands:
    * snapshot
    * upload
    * diff
    * download
"""
import os
from pathlib import Path
import concurrent.futures
import json

import click

from deadline.client import api
from deadline.job_attachments.upload import FileStatus, S3AssetManager, S3AssetUploader
from deadline.job_attachments.models import JobAttachmentS3Settings, AssetRootManifest, BaseManifestPath
from deadline.job_attachments.asset_manifests.decode import decode_manifest

from deadline.job_attachments.caches import HashCache

from .._common import _apply_cli_options_to_config, _handle_error, _ProgressBarCallbackManager
from ...exceptions import NonValidInputError, ManifestOutdatedError
from ...config import get_setting, config_file


@click.group(name="asset")
@_handle_error
def cli_asset():
    """
    Commands to work with AWS Deadline Cloud Job Attachments.
    """


@cli_asset.command(name="snapshot")
@click.option("--root-dir", required=True, help="The root directory to snapshot. ")
@click.option("--manifest-out", help="Destination path to directory for created manifest. ")
@click.option(
    "--recursive",
    "-r",
    help="Flag to recursively snapshot subdirectories. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_snapshot(root_dir: str, manifest_out: str, recursive: bool, **args):
    """
    Creates manifest of files specified root directory.
    """
    if not os.path.isdir(root_dir):
        raise NonValidInputError(f"Specified root directory {root_dir} does not exist. ")

    if manifest_out and not os.path.isdir(manifest_out):
        raise NonValidInputError(f"Specified destination directory {manifest_out} does not exist. ")
    elif manifest_out is None:
        manifest_out = root_dir
        click.echo(f"Manifest creation path defaulted to {root_dir} \n")

    inputs = []
    for root, dirs, files in os.walk(root_dir):
        inputs.extend([str(os.path.join(root, file)) for file in files])
        if not recursive:
            break

    # Placeholder Asset Manager
    asset_manager = S3AssetManager(
        farm_id=" ", queue_id=" ", job_attachment_settings=JobAttachmentS3Settings(" ", " ")
    )
    asset_uploader = S3AssetUploader()
    hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

    upload_group = asset_manager.prepare_paths_for_upload(
        input_paths=inputs, output_paths=[root_dir], referenced_paths=[]
    )
    if upload_group.asset_groups:
        _, manifests = api.hash_attachments(
            asset_manager=asset_manager,
            asset_groups=upload_group.asset_groups,
            total_input_files=upload_group.total_input_files,
            total_input_bytes=upload_group.total_input_bytes,
            print_function_callback=click.echo,
            hashing_progress_callback=hash_callback_manager.callback,
        )

    # Write created manifest into local file, at the specified location at manifest_out
    for asset_root_manifests in manifests:
        if asset_root_manifests.asset_manifest is None:
            continue
        source_root = Path(asset_root_manifests.root_path)
        file_system_location_name = asset_root_manifests.file_system_location_name
        (_, _, manifest_name) = asset_uploader._gather_upload_metadata(
            manifest=asset_root_manifests.asset_manifest,
            source_root=source_root,
            file_system_location_name=file_system_location_name,
        )
        asset_uploader._write_local_input_manifest(
            manifest_write_dir=manifest_out,
            manifest_name=manifest_name,
            manifest=asset_root_manifests.asset_manifest,
            root_dir_name=os.path.basename(root_dir),
        )

    click.echo(f"Manifest created at {manifest_out}\n")


@cli_asset.command(name="upload")
@click.option("--root-dir", help="The root directory of assets to upload. ")
@click.option(
    "--manifest",
    required=True,
    help="The path to manifest folder of the directory specified for upload. ",
)
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use. ")
@click.option(
    "--update",
    help="Flag to update manifest before upload. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_upload(root_dir: str, manifest: str, update: bool, **args):
    """
    Uploads the assets in the provided manifest file to S3.
    """
    # test:
    # - farm-id queue-id missing w / wo config
    # - upload correct manifest / data
    # - test auto update

    # need:
    # - case for valid manifest path but no manifest
    # - case for invalid manifest path, path does not exist

    # if need to update manifets
    # -> prompt for --update

    # if no --root-dir
    #   -> use default root dir where manifest lives, could fail
    asset_root_dir = Path(manifest).parent
    print(asset_root_dir)

    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)
    upload_callback_manager = _ProgressBarCallbackManager(length=100, label="Uploading Attachments")

    deadline = api.get_boto3_client("deadline", config=config)
    queue_id = get_setting("defaults.queue_id", config=config)
    farm_id = get_setting("defaults.farm_id", config=config)

    queue = deadline.get_queue(
        farmId=farm_id,
        queueId=queue_id,
    )

    # assume queue role - session permissions
    queue_role_session = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    asset_manager = S3AssetManager(
        farm_id=farm_id,
        queue_id=queue_id,
        job_attachment_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
        session=queue_role_session,
    )

    asset_uploader = S3AssetUploader()

    # def check manifest for updates
    # how does JA upload check manifest to change ?
    # check if local files have changed since manifest

    # read local manifest into BaseAssetManifest object
    asset_manifest = None
    for filename in os.listdir(manifest):
        if filename.endswith("_input"):
            filepath = os.path.join(manifest, filename)
            with open(filepath, "r") as input_file:
                manifest_data_str = input_file.read()
                asset_manifest = decode_manifest(manifest_data_str)

                # print("asset_manifest: ", asset_manifest)



    asset_root_manifests: list[AssetRootManifest] = []
    asset_root_manifests.append(
        AssetRootManifest(
            root_path=asset_root_dir,
            asset_manifest=asset_manifest,
        )
    )

    # 
    directory_manifest_changes = get_directory_manifest_changes(asset_manager=asset_manager, asset_root_manifest=asset_root_manifests[0], manifest=manifest, update=update)
    directory_manifest_changes_modified_only = []
    for file_status, manifest_path in directory_manifest_changes:
        if file_status is FileStatus.MODIFIED:
            directory_manifest_changes_modified_only.append((file_status, manifest_path))

    print("changes: ", directory_manifest_changes_modified_only)

    # must update modified files, will either auto --update manifest or prompt user of file discrepancy
    if len(directory_manifest_changes_modified_only) > 0:
        if update:
            # calls snapshot to update hashes of manifest
            
            None
        else:
            raise ManifestOutdatedError(f"Manifest contents are outdated; versioning does not match local files in {asset_root_dir}. Please run with --update to fix current files. ")

    attachment_settings = api.upload_attachments(
        asset_manager=asset_manager,
        manifests=asset_root_manifests,
        print_function_callback=click.echo,
        upload_progress_callback=upload_callback_manager.callback,
    )

    full_manifest_key = attachment_settings["manifests"][0]["inputManifestPath"]
    manifest_name = os.path.basename(full_manifest_key)
    manifest_dir_name = os.path.basename(manifest)
    asset_uploader._write_local_manifest_s3_mapping(
        manifest_write_dir=asset_root_dir,
        manifest_name=manifest_name,
        full_manifest_key=full_manifest_key,
        manifest_dir_name=manifest_dir_name,
    )

    click.echo(f"Upload of {asset_root_dir} complete. ")


@cli_asset.command(name="diff")
@click.option("--root-dir", help="The root directory to compare changes to. ")
@click.option("--manifest", help="The path to manifest folder of directory to show changes of. ")
@click.option(
    "--print",
    help="Pretty prints diff information. ",
    is_flag=True,
    show_default=True,
    default=False,
)
@_handle_error
def asset_diff(**args):
    """
    Check file differences of a directory since last snapshot.

    TODO: show example of diff output
    """
    read_manifest_data(
        "/Users/stangch/Desktop/maya_wrench_sample/maya_wrench_sample_manifests/eca77a3a0ba1f477b7f6cc397494b424_input"
    )
    click.echo("diff shown")


@cli_asset.command(name="download")
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use.")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--job-id", help="The AWS Deadline Cloud Job to get. ")
@_handle_error
def asset_download(**args):
    """
    Downloads input manifest of previously submitted job.
    """
    click.echo("download complete")


def read_manifest_data(manifest_path) -> list[tuple]:
    """
    Read specified manifest, parses file data it contains, and returns the data of each entry.
    """
    data_paths = []

    with open(manifest_path, "r") as manifest_file:
        manifest_data = json.load(manifest_file)
        entries = manifest_data["paths"]
        for entry in entries:
            print(entry)

    return data_paths

def get_directory_changes():
    """
    TODO: gets changes of a directory
    """
    None

def get_manifest_changes():
    """
    
    """
    # used by upload, to get modified files of a manifest
    None


# but do we diff against directory vs manifest or directory vs hash cache ????
def get_file_changes(
    asset_manager: S3AssetManager, asset_root_manifest: AssetRootManifest, manifest: str, update: bool
) -> list[(FileStatus, BaseManifestPath)]:
    """
    Checks a manifest file, compares it to specified root directory or manifest of files with the local hash cache. 
    Returns a list of tuples containing the file information, and its corresponding file status
    """
    cache_config = config_file.get_cache_directory()

    root_path = asset_root_manifest.root_path


    input_paths = []
    for root, dirs, files in os.walk(root_path):
        if os.path.samefile(root, manifest):
            dirs[:] = []
            continue
        for filename in files:
            file_path = os.path.join(root, filename)
            input_paths.append(file_path)

    print("input: ", input_paths)


    with HashCache(cache_config) as hash_cache:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(asset_manager._process_input_path, path=Path(root_path, path), root_path=root_path, hash_cache=hash_cache, update=update): path
                for path in input_paths
            }
            new_or_modified_paths: list[(FileStatus, BaseManifestPath)] = []
            for future in concurrent.futures.as_completed(futures):
                (file_status, _, manifestPath) = future.result()
                if file_status is FileStatus.NEW or file_status is FileStatus.MODIFIED:
                    new_or_modified_paths.append((file_status, manifestPath))

            return new_or_modified_paths

"""
tldr, we want upload to only upload whats in the manifest. If there are modifications, we update the mods only. if users want to add
more files, they must snapshot. 

upload -> check manifest paths -> grab local files from manifest paths -> update or not update -> upload
"""