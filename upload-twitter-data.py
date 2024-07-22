import pandas as pd
from google.cloud import storage

def read_csv_files_from_bucket(bucket_name):
    col_sz_dict = {}
    idx_dict = {}

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Get bucket
    bucket = storage_client.bucket(bucket_name)

    destination_prefix = "weird_index/"

    # Use glob to find all CSV files in the bucket
    itr = 0
    blob_prefix = 'long/'
    blobs = bucket.list_blobs(prefix=blob_prefix)
    # print("Blobs:")
    # for blob in blobs:
    #     print(blob.name)
    #     itr += 1
    # print(itr)

    for blob in blobs:
        with blob.open("r") as f:
            # num_cols = len(f.readline().split(","))
            line = f.readline().split(",")
            if '"extractedts"' in line:
                col = '"extractedts"'
            else:
                col = '"extractedts"\n'

            idx = line.index(col)
            if idx in idx_dict.keys():
                idx_dict[idx] += 1
            else:
                idx_dict[idx] = 1

        #     if num_cols in col_sz_dict.keys():
        #         col_sz_dict[num_cols] += 1
        #     else:
        #         col_sz_dict[num_cols] = 1
        if idx == 17:
        
         #   Construct new blob name with destination prefix
            new_blob_name = destination_prefix + blob.name[len(blob_prefix):]

                # Copy blob to the new destination
            new_blob = bucket.copy_blob(blob, bucket, new_blob_name)

                # Delete the original blob if needed
            if new_blob:
                blob.delete()

                print(f"Moved {blob.name} to {new_blob_name}")
        # # Read CSV file
        # # csv_content = blob.download_as_string().decode('utf-8')
        
        # # # Process CSV content (example: print the content)
        # # print(csv_content)
   # print(col_sz_dict)
    print(idx_dict)


read_csv_files_from_bucket("ukraine-twitter-data")
# 461 files
# {18: 55, 29: 406}