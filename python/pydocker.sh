#!/bin/bash

docker build -t asypython . 
docker run -it --rm -v /research/asymmetric_policy/data:/app/data/  asypython