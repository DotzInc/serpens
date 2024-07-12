import os
import unittest
from unittest.mock import patch

from serpens.storages import StorageClient


class TestStorages(unittest.TestCase):
    def setUp(self):
        self.patch_boto3 = patch("serpens.s3.boto3")
        self.mock_boto3 = self.patch_boto3.start()
        self.s3_client = self.mock_boto3.client.return_value

        self.patch_gcs = patch("serpens.cloud_storage.storage")
        self.mock_gcs = self.patch_gcs.start()
        self.gcs_client = self.mock_gcs.Client.return_value

        self.bucket = ""
        self.key = ""

    def tearDown(self):
        self.patch_boto3.stop()
        self.patch_gcs.stop()

    @patch.dict(os.environ, {"STORAGE_PROVIDER": "s3"})
    def test_get_object_s3(self):
        StorageClient.instance().get(self.bucket, self.key)

        self.s3_client.get_object.assert_called_once_with(Bucket=self.bucket, Key=self.key)

    @patch.dict(os.environ, {"STORAGE_PROVIDER": "cloud_storage"})
    def test_upload_object_gcs(self):
        StorageClient.instance().upload("foo", self.bucket, self.key, "image/jpeg")

        blob = self.gcs_client.bucket.return_value.blob.return_value

        blob.upload_from_string.assert_called_once_with(
            "foo", content_type="image/jpeg", predefined_acl="private"
        )
