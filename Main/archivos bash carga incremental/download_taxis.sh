#!/bin/bash

#export YEAR=${YEAR:=2015}
SOURCE=https://d37ci6vzurychx.cloudfront.net/trip-data

if test "$#" -ne 2; then
   echo "Usage: ./download.sh year month"
   echo "   eg: ./download.sh 2015 01"
   exit
fi

YEAR=$1
MONTH=$2

OUTDIR=taxis_raw
mkdir -p $OUTDIR

for COLOR in green yellow; do
      FILE=${COLOR}_tripdata_${YEAR}-${MONTH}.parquet
      TEMP_FILE=${OUTDIR}/temp_${FILE}
      
      # Download the file to a temporary location
      curl -k -o ${TEMP_FILE} ${SOURCE}/${FILE}
      
      # Get the size of the downloaded file
      FILE_SIZE=$(stat -c %s ${TEMP_FILE})
      
      # Check if the file size is greater than 2KB (2000 bytes)
      if [ "$FILE_SIZE" -gt 2000 ]; then
        # Move the temporary file to the final location
        mv ${TEMP_FILE} ${OUTDIR}/${FILE}
      else
        # Delete the temporary file if size is less than or equal to 2KB
        rm ${TEMP_FILE}
      fi
done
