#!/bin/sh

$S3_BUCKET=$BUCKET

#Take the enron example jars and put them into an S3 bucket.
HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

s3cp $HERE/emr-bootstrap.sh s3://$S3_BUCKET/emr-bootstrap.sh
s3mod s3://$S3_BUCKET/emr-bootstrap.sh public-read

s3cp $HERE/RemoveDeadEnds/eraseDeadEndsAWSIterate.pig s3://$S3_BUCKET/eraseDeadEndsAWSIterate.pig
s3mod s3://$S3_BUCKET/eraseDeadEndsAWSIterate.pig public-read
s3cp $HERE/RemoveDeadEnds/myudf/myudf.jar s3://$S3_BUCKET/myudf.jar
s3mod s3://$S3_BUCKET/myudf.jar public-read

s3cp $HERE/PreFormat/PreFormat.jar s3://$S3_BUCKET/PreFormat.jar
s3mod s3://$S3_BUCKET/PreFormat.jar public-read

s3cp $HERE/PageRank/PageRank.jar s3://$S3_BUCKET/PageRank.jar
s3mod s3://$S3_BUCKET/PageRank.jar public-read
