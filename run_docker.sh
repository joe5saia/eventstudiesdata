#!/bin/bash

docker build -t eventstudies . 
docker run -it --rm -v $(pwd)/data/processed/:/app/output/ -u `id -u $USER`:`id -g $USER` eventstudies 

