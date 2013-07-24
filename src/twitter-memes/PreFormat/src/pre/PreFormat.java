package pre;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PreFormat {

	public static double TOTAL_NODES = 1644221;
	
	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "preformat");
		
		job.setJarByClass(PreFormat.class);
		job.setMapperClass(PreFormatMapper.class);
		job.setReducerClass(PreFormatReducer.class);

		job.setInputFormatClass(com.mongodb.hadoop.BSONFileInputFormat.class);
		job.setOutputFormatClass(com.mongodb.hadoop.BSONFileOutputFormat.class);

		job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setOutputValueClass(com.mongodb.hadoop.io.BSONWritable.class);
		
		// A bit of a hack, it wouldn't 
		String inputString = "s3n://memes-bson/output/erasedDeadEnds.bson";
		FileInputFormat.addInputPath(job, new Path(inputString));
		String outputString = "s3n://memes-bson/output/preFormatted.bson";
		job.getConfiguration().set("mapred.output.file", outputString);
		
		job.waitForCompletion(true);
	}
}
