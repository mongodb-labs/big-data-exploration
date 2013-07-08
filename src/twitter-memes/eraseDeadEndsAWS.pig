REGISTER s3://memes-bson/mongo-2.10.1.jar
REGISTER s3://memes-bson/mongo-hadoop-pig_1.1.2-1.1.0.jar
REGISTER s3://memes-bson/mongo-hadoop-core_1.1.2-1.1.0.jar
REGISTER s3://memes-bson/mongo-hadoop_1.1.2-1.1.0.jar
REGISTER s3://memes-bson/myudf.jar

ORIGINAL =  LOAD '$INPUT'
            USING com.mongodb.hadoop.pig.BSONLoader;

OUTS =      foreach ORIGINAL generate $0#'_id' as url, $0#'value'#'links' as links;

SHOWING = foreach OUTS generate url, FLATTEN(myudf.MYBAG(links)) as single;

JOINED =    JOIN OUTS by url, SHOWING by single;

PROJECT =   foreach JOINED generate SHOWING::url as url, SHOWING::single as single;

TOGETHER =  GROUP PROJECT by url;

RESULT =    foreach TOGETHER generate $0 as url, $1.single as links; 

STORE RESULT INTO '$OUTPUT' 
    USING com.mongodb.hadoop.pig.BSONStorage;
