#!/bin/sh

S3_BUCKET=$BUCKET

elastic-mapreduce --create --jobflow PreFormat \
    --name "Preformat" \
    --instance-type m1.xlarge \
    --num-instances 5 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/PreFormat.jar \
    --arg -D --arg mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat \
    --arg -D --arg mapred.input.dir=s3n://<S3 PATH TO YOUR INPUT> \
    --arg -D --arg mongo.job.mapper=mapreduce.PreFormatMapper \
    --arg -D --arg mongo.job.reducer=mapreduce.PreFormatReducer \
    --arg -D --arg mongo.job.output.key=org.apache.hadoop.io.Text \
    --arg -D --arg mongo.job.output.value=com.mongodb.hadoop.io.BSONWritable \
    --arg -D --arg bson.pathfilter.class=com.mongodb.hadoop.BSONPathFilter \
    --arg -D --arg mapred.output.dir=s3n://$S3_BUCKET/<PATH TO YOUR OUTPUT> \
    --arg -D --arg mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat \
    --arg -D --arg totalNodes=36814086 \
    --arg -D --arg bson.output.build_splits=true \
