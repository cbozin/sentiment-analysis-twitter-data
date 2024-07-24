from google.cloud import storage

def read_csv_files_from_bucket(bucket_name):
    col_sz_dict = {}
    idx_dict = {}

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Get bucket
    bucket = storage_client.bucket(bucket_name
    destination_prefix = "weird_index/"

    # find all CSV files in the bucket
    itr = 0
    blob_prefix = 'long/'
    blobs = bucket.list_blobs(prefix=blob_prefix)

    for blob in blobs:
        with blob.open("r") as f:

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

        if idx == 17:
        
         #   Construct new blob name with destination prefix
            new_blob_name = destination_prefix + blob.name[len(blob_prefix):]

                # Copy blob to the new destination
            new_blob = bucket.copy_blob(blob, bucket, new_blob_name)

                # Delete the original blob if needed
            if new_blob:
                blob.delete()

                print(f"Moved {blob.name} to {new_blob_name}")

    print(idx_dict)


read_csv_files_from_bucket("ukraine-twitter-data")
# 461 files
# {18: 55, 29: 406}
