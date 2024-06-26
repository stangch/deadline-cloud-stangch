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
import json

import click

from deadline.client import api
from deadline.job_attachments.upload import S3AssetManager, S3AssetUploader
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.asset_manifests.decode import decode_manifest

from .._common import _apply_cli_options_to_config, _handle_error, _ProgressBarCallbackManager
from ...exceptions import NonValidInputError
from ...config import get_setting

IGNORE_FILE: str = "manifests"


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
def asset_snapshot(recursive, **args):
    """
    Creates manifest of files specified root directory.
    """
    root_dir = args.pop("root_dir")
    root_dir_basename = os.path.basename(root_dir) + "_"
    out_dir = args.pop("manifest_out")

    if not os.path.isdir(root_dir):
        misconfigured_directories_msg = f"Specified root directory {root_dir} does not exist. "
        raise NonValidInputError(misconfigured_directories_msg)

    if out_dir and not os.path.isdir(out_dir):
        misconfigured_directories_msg = (
            f"Specified destination directory {out_dir} does not exist. "
        )
        raise NonValidInputError(misconfigured_directories_msg)
    elif out_dir is None:
        out_dir = root_dir

    inputs = []
    for root, dirs, files in os.walk(root_dir):
        if os.path.basename(root).endswith("_manifests"):
            continue
        for file in files:
            file_full_path = str(os.path.join(root, file))
            inputs.append(file_full_path)
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

    # Write created manifest into local file, at the specified location at out_dir
    for asset_root_manifests in manifests:
        print(asset_root_manifests.asset_manifest)
        if asset_root_manifests.asset_manifest is None:
            continue
        source_root = Path(asset_root_manifests.root_path)
        file_system_location_name = asset_root_manifests.file_system_location_name
        (_, _, manifest_name) = asset_uploader._gather_upload_metadata(
            asset_root_manifests.asset_manifest, source_root, file_system_location_name
        )
        asset_uploader._write_local_input_manifest(
            out_dir, manifest_name, asset_root_manifests.asset_manifest, root_dir_basename
        )


@cli_asset.command(name="upload")
@click.option("--root-dir", help="The root directory of assets to upload. ")
@click.option(
    "--manifest", required=True, help="The path to manifest folder of the directory specified for upload. "
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
def asset_upload(root_dir, manifest, update, **args):
    """
    Uploads the assets in the provided manifest file to S3.
    """
    # take config values of farm and queue
        # what do current commands do ?
        # failure to proved farm/queue ?
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
    
    asset_manifest = None

    for filename in os.listdir(manifest):
        if filename.endswith("_input"):
            filepath = os.path.join(manifest, filename)
            with open(filepath, "r") as input_file:
                manifest_data_str = input_file.read()
                asset_manifest = decode_manifest(manifest_data_str)

                print("asset_manifest: ", asset_manifest)

    # needs asset group ?
        # prepare_paths_for_upload -> upload groups

    
    asset_root_manifests: list[AssetRootManifest] = []
    asset_root_manifests.append(
                AssetRootManifest(
                    file_system_location_name=group.file_system_location_name,
                    root_path=group.root_path,
                    asset_manifest=asset_manifest,
                    outputs=sorted(list(group.outputs)),
                ))

    attachment_settings = api.upload_attachments(
        asset_manager=asset_manager,
        manifests=[asset_manifest],
        print_function_callback=click.echo,
        upload_progress_callback=upload_callback_manager.callback,
        )

    print(attachment_settings)
    #S3 mapping


    # prompt if changes needed
    # --update for auto update snapshot

    click.echo("upload done")


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
    read_manifest_data("/Users/stangch/Desktop/maya_wrench_sample/maya_wrench_sample_manifests/eca77a3a0ba1f477b7f6cc397494b424_input")
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

