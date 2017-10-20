#!/bin/bash

# Pull docker image and start in development mode
docker pull docker.elastic.co/elasticsearch/elasticsearch:5.6.3

docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" \
           -e "transport.host=0.0.0.0" -e "xpack.security.enabled=false" \
           docker.elastic.co/elasticsearch/elasticsearch:5.6.3
