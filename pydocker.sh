#!/bin/bash

docker build -t asypython . 
docker run -it --rm -v /research/eventstudiesdata/data/processed/:/app/data/  asypython
