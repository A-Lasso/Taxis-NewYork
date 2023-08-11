#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./upload.sh  destination-bucket-name"
    exit
fi

BUCKET=$1

echo "Uploading to bucket $BUCKET..."
gsutil -m cp taxis_raw/*.parquet gs://$BUCKET/taxis/