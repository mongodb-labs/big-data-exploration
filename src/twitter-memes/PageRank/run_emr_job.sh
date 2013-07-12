S3_BUCKET=memes-bson

elastic-mapreduce --create --jobflow NoDeadEnds \
    --name "No Dead Ends - PageRank nodes" \
    --instance-type m1.large \
    --num-instances 1 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/allMemesPageRank.jar \
    --arg 0 \
    --arg s3n://$S3_BUCKET/output/originalMemes/format/ \
    --arg s3n://$S3_BUCKET/output/originalMemes/pagerank/ \
    --arg -D --arg bson.output.build_splits=true \
