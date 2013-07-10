S3_BUCKET=memes-bson

elastic-mapreduce --create --jobflow PreFormat8 \
    --name "All Memes = Preformat 8" \
    --instance-type m1.xlarge \
    --num-instances 8 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/PreFormatDir.jar \
    --arg -D --arg mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat \
    --arg -D --arg mapred.input.dir=s3n://$S3_BUCKET/dump/twitter/original \
    --arg -D --arg mongo.job.mapper=pre.PreFormatMapper \
    --arg -D --arg mongo.job.reducer=pre.PreFormatReducer \
    --arg -D --arg mongo.job.output.key=org.apache.hadoop.io.Text \
    --arg -D --arg mongo.job.output.value=com.mongodb.hadoop.io.BSONWritable \
    --arg -D --arg bson.pathfilter.class=com.mongodb.hadoop.BSONPathFilter \
    --arg -D --arg mapred.output.dir=s3n://$S3_BUCKET/output/originalMemes \
    --arg -D --arg mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat \
    --arg -D --arg bson.output.build_splits=true
