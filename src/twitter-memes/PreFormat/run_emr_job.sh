S3_BUCKET=memes-bson

elastic-mapreduce --create --jobflow PreFormat8 \
    --name "No Dead Ends - Preformat Dir" \
    --instance-type m1.large \
    --num-instances 5 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/PreFormat.jar \
    --arg -D --arg mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat \
    --arg -D --arg mapred.input.dir=s3n://$S3_BUCKET/output/erasedDeadEnds70.bson \
    --arg -D --arg mongo.job.mapper=nodeadends.NoDeadEndsMapper \
    --arg -D --arg mongo.job.reducer=nodeadends.NoDeadEndsReducer \
    --arg -D --arg mongo.job.output.key=org.apache.hadoop.io.Text \
    --arg -D --arg mongo.job.output.value=com.mongodb.hadoop.io.BSONWritable \
    --arg -D --arg bson.pathfilter.class=com.mongodb.hadoop.BSONPathFilter \
    --arg -D --arg mapred.output.dir=s3n://$S3_BUCKET/output/noDeadEnds/format/ \
    --arg -D --arg mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat \
    --arg -D --arg bson.output.build_splits=true \
