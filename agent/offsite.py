"""Clients for the offsite storage that press picks for a backup.

Press sends `auth` with ACCESS_KEY, SECRET_KEY, REGION, PROVIDER and ENDPOINT_URL.
PROVIDER is press's label for the Backup Bucket: "AWS S3" or "OCI".
"""

from __future__ import annotations


def get_s3_client(auth: dict):
    import boto3

    options = {
        "aws_access_key_id": auth["ACCESS_KEY"],
        "aws_secret_access_key": auth["SECRET_KEY"],
    }
    if auth.get("REGION"):
        options["region_name"] = auth["REGION"]
    # AWS keeps boto3's own endpoint, so an old endpoint on a press record changes nothing
    if auth.get("PROVIDER") == "OCI":
        from botocore.config import Config

        options["endpoint_url"] = get_oci_endpoint(auth)
        # OCI buckets with namespace scope only answer path-style requests
        options["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client("s3", **options)


def get_rclone_provider(auth: dict) -> tuple[str, str]:
    """rclone's --s3-provider value and the endpoint. Raises KeyError for an unknown PROVIDER."""
    if auth["PROVIDER"] == "AWS S3":
        return "AWS", f"s3.{auth['REGION']}.amazonaws.com"
    if auth["PROVIDER"] == "OCI":
        # rclone has no OCI entry for S3, so it goes as "Other" with OCI's S3 endpoint
        return "Other", get_oci_endpoint(auth)
    raise KeyError(auth["PROVIDER"])


def get_oci_endpoint(auth: dict) -> str:
    # Without it boto3 and rclone fall back to AWS, so fail instead
    if not auth.get("ENDPOINT_URL"):
        raise ValueError("OCI offsite storage needs ENDPOINT_URL")
    return auth["ENDPOINT_URL"]
