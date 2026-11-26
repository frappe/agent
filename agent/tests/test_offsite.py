from __future__ import annotations

import unittest

from agent.offsite import get_rclone_provider, get_s3_client

OCI_ENDPOINT = "https://namespace.compat.objectstorage.me-jeddah-1.oraclecloud.com"


def offsite_auth(provider: str, region: str, endpoint_url: str | None) -> dict:
    return {
        "ACCESS_KEY": "access",
        "SECRET_KEY": "secret",
        "REGION": region,
        "PROVIDER": provider,
        "ENDPOINT_URL": endpoint_url,
    }


class TestOffsiteS3Client(unittest.TestCase):
    def test_oci_client_sends_requests_to_the_oci_endpoint_in_the_bucket_region(self):
        client = get_s3_client(offsite_auth("OCI", "me-jeddah-1", OCI_ENDPOINT))

        self.assertEqual(client.meta.endpoint_url, OCI_ENDPOINT)
        self.assertEqual(client.meta.region_name, "me-jeddah-1")

    def test_oci_client_puts_the_bucket_in_the_path_not_the_host(self):
        client = get_s3_client(offsite_auth("OCI", "me-jeddah-1", OCI_ENDPOINT))

        url = client.generate_presigned_url("get_object", Params={"Bucket": "backups", "Key": "a.gz"})
        self.assertTrue(url.startswith(f"{OCI_ENDPOINT}/backups/a.gz"), url)

    def test_aws_client_ignores_the_endpoint_from_press_and_uses_the_aws_one(self):
        client = get_s3_client(offsite_auth("AWS S3", "ap-south-1", "https://old.endpoint.example"))

        self.assertEqual(client.meta.endpoint_url, "https://s3.ap-south-1.amazonaws.com")

    def test_oci_without_an_endpoint_fails_instead_of_uploading_to_aws(self):
        with self.assertRaisesRegex(ValueError, "OCI offsite storage needs ENDPOINT_URL"):
            get_s3_client(offsite_auth("OCI", "me-jeddah-1", None))


class TestOffsiteRcloneProvider(unittest.TestCase):
    def test_aws_maps_to_the_aws_provider_and_regional_endpoint(self):
        self.assertEqual(
            get_rclone_provider(offsite_auth("AWS S3", "ap-south-1", None)),
            ("AWS", "s3.ap-south-1.amazonaws.com"),
        )

    def test_oci_maps_to_the_other_provider_and_the_oci_endpoint(self):
        self.assertEqual(
            get_rclone_provider(offsite_auth("OCI", "me-jeddah-1", OCI_ENDPOINT)),
            ("Other", OCI_ENDPOINT),
        )

    def test_unknown_provider_raises_key_error_so_the_job_fails_with_a_clear_message(self):
        with self.assertRaisesRegex(KeyError, "Wasabi"):
            get_rclone_provider(offsite_auth("Wasabi", "us-east-1", None))
