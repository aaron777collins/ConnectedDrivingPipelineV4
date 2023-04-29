#!/bin/bash
# Download data from the web
curl -L https://data.transportation.gov/api/views/9k4m-a3jc/rows.csv?accessType=DOWNLOAD -o data/data.csv
