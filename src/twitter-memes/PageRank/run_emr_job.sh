S3_BUCKET=memes-bson

elastic-mapreduce --create --jobflow NoDeadEnds \
    --name "No Dead Ends - PageRank" \
    --instance-type m1.xlarge \
    --num-instances 8 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/noDeadEndsPageRank.jar \
    --arg 1 \
    --arg s3n://$S3_BUCKET/output/noDeadEnds/format/ \
    --arg s3n://$S3_BUCKET/output/noDeadEnds/pagerank/ \
    --arg -D --arg bson.output.build_splits=true \
