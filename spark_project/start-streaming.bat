#! /bin/bash

docker build --rm -t bde/spark-app .
docker run --name SparkStreaming --net bde -p 4040:4040 bde/spark-app