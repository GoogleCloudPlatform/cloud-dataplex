# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import storage

def check_bucket_permission(bucket_name) -> bool:
    '''
        Method to check the bucket storage permission
    '''
    try:
        permission = 'storage.buckets.get'
        # Initialize storage client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Use the testIamPermissions method to check permissions
        permissions_to_test = [permission]
        permissions = bucket.test_iam_permissions(permissions_to_test)

        # Check if the permission is granted
        if permission in permissions:
            return True
        else:
            return False
    except Exception as error:
        print(f'Bucket {bucket_name} does not exists. ')
        print(error)
        return None
