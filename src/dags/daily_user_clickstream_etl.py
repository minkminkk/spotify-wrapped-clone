#TODO: 
# - Use MongoClient and utils.faker_custom_provider to ingest data into MongoDB
# - Use Spotify tracks dataset to get available track_ids 
# and use it as sample for faking data
# (https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset)
#       - Download dataset and use Airflow to save track_id column as another file
#       - When generating data, read data from that file