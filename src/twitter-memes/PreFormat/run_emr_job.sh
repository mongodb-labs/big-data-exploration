
elastic-mapreduce --create --jobflow PreFormat5 \
    --instance-type m1.small \
    --num-instances 1 \
    --bootstrap-action s3://$S3_BUCKET/emr-bootstrap.sh \
    --log-uri s3://$S3_BUCKET/logs \
    --jar s3://$S3_BUCKET/PreFormat.jar \
    #--arg -D --arg mongo.job.input.format=com.mongodb.hadoop.BSONFileInputFormat \
    --arg -D --arg mapred.input.dir=s3n://$S3_BUCKET/input/5.bson \
    #--arg -D --arg mongo.job.mapper=com.mongodb.hadoop.examples.enron.EnronMailMapper \
    #--arg -D --arg mongo.job.output.key=com.mongodb.hadoop.examples.enron.MailPair \
    #--arg -D --arg mongo.job.output.value=org.apache.hadoop.io.IntWritable \
    --arg mongo.job.partitioner= \
    #--arg -D --arg mongo.job.reducer=com.mongodb.hadoop.examples.enron.EnronMailReducer \
    --arg mongo.job.sort_comparator= \
    --arg mongo.job.background= \
    --arg -D --arg mapred.output.file=s3n://$S3_BUCKET/output/12.bson 
    #--arg -D --arg mongo.job.output.format=com.mongodb.hadoop.BSONFileOutputFormat \
    #--arg -D --arg mapred.child.java.opts=-Xmx2048m
    #--arg -D --arg mapred.task.profile=true \
