#!/bin/sh

S3_BUCKET=$BUCKET

elastic-mapreduce --create --jobflow PageRank \
    --name "PageRank" \
    --instance-type m1.xlarge \
    --num-instances 10 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/PageRank.jar \
    --arg s3n://$S3_BUCKET/<PATH TO INPUT DIRECTORY> \
    --arg s3n://$S3_BUCKET/<PATH TO OUTPUT DIRECTORY> \
    --arg -D --arg bson.output.build_splits=true \
