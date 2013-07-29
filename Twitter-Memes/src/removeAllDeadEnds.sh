#!/bin/bash

# Program Requirements:
# 1. s3cmd -> (download here: http://s3tools.org/s3cmd)
#          command line S3 client
# 2. elastic-map-reduce client -> 
#         (instructions and download file here: https://github.com/tc/elastic-mapreduce-ruby)
# 3. cut bash program -> should already be installed on most unix/linux servers

# Goal:
# Runs 'eraseDeadEndsAWSIterate.pig' repeatedly on AWS
# until no dead ends have been removed or until
# maxiterations has been reached

# File Requirements:
# Bucket you want to write to must have an output folder containing
# at least 1 BSON file:

# max iterations of removeDeadEnds.pig
MAX_ITER=200

## Edit me: bucket to store results in
# bucket must have an output sub-directory
# containing at least one BSON file to act on
# for example:
# /memes-bson 
#    /eraseDeadEndsAWSIterate.pig -- pig script to run
#    /emr-bootstrap.sh -- bootstrap file to copy mongo, mongo-hadoop jar files to hadoop classpath
#    /mongo-hadoop_core-<x.x.x>.jar
#    /mongo-java_driver-<x.x.x>.jar
#    /<other files -- udfs and such>
#    /output
#       /erasedDeadEnds0.bson
#          
BUCKET="memes-bson"

# elastic-mapreduce path
EMR_PATH="elastic-mapreduce-ruby/elastic-mapreduce"

# s3cmd path
S3CMD_PATH="s3cmd"

# command to check if mapreduce job still running
RUNNING= $EMR_PATH" --list | head -n 1 | grep -ivE 'completed|terminated|failed|cancelled'"

# sleep time (in seconds) for polling to check if JOB has finished
SLEEP_TIME=10

# store size of current and previous bson files
fsize=1
ffsize=0

# runOnce ->
# creates a job flow on amazon Elastic Map Reduce using the ruby client
function runOnce {
    echo "In the $(($2))th iteration of 'Remove Dead Ends'..."

    $EMR_PATH --create --jobflow ERASEDEADENDS \
	--name "Erase Dead Ends $2" \
	--instance-type m1.xlarge \
	--bootstrap-action s3://$1/emr-bootstrap.sh \
	--log-uri s3://$1/logs \
        --pig-script s3://$1/eraseDeadEndsAWSIterate.pig \
	--args -p,INPUT=s3://$1/output/erasedDeadEnds$2.bson \
	--args -p,OUTPUT=s3://$1/output/erasedDeadEnds$(($2 + 1)).bson
}


i=0
# first run of pig job
runOnce $BUCKET $i

while [ "$i" -lt "$MAX_ITER" ]; do
    result=$(eval $RUNNING)

    while [ -n "$result" ]; do
	# sleep for 10 secs
	sleep $SLEEP_TIME
	result=$(eval $RUNNING)
    done

    i=$((i+1))
    
    fsize=$($S3CMD_PATH du s3://$BUCKET/output/erasedDeadEnds$i.bson | cut -d " " -f1)

    if [ $fsize == $ffsize ]; 
    then
	echo "Filesize remained the same. Done."
	exit
    else
	# run job again
	runOnce $BUCKET $i
    fi

    # store former size
    ffsize=$fsize
done

