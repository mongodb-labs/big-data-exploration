import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

public class RanPageRank extends Configured implements Tool{
	
	public static double beta = 0.9;
	public static double numNodes = 1644221;
	public static double distributedBeta = (1 - beta) / numNodes;
	public static double threshold = 0.001 * numNodes;
	public static String inputString = "s3n://memes-bson/output/preFormatted.bson";
	public static String outputString = "s3n://memes-bson/output/";
	
	static enum RanCounters {RESIDUAL};
	
	@Override
	public int run(String[] arg0) throws Exception {
		StringBuilder sb = new StringBuilder();
		String numTrials = arg0[0];
		
		try {
			Configuration conf;
			Job job;
			
			for (int i = 0 ; i < 50 ; i++) {
				int last = i - 1;
				conf = new Configuration();
				job = new Job(conf, "ran-pagerank");
				
				job.setJarByClass(RanPageRank.class);
				job.setMapperClass(RanMapper.class);
				job.setReducerClass(RanReducer.class);
		
				job.setInputFormatClass(com.mongodb.hadoop.BSONFileInputFormat.class);
				job.setOutputFormatClass(com.mongodb.hadoop.BSONFileOutputFormat.class);
		
				job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
				job.setOutputValueClass(com.mongodb.hadoop.io.BSONWritable.class);
				
				if (i == 0) {
					FileInputFormat.addInputPath(job, new Path(inputString));
				} else {
					FileInputFormat.addInputPath(job, new Path(outputString + numTrials + last + ".bson"));
				}
				job.getConfiguration().set("mapred.output.file", outputString + numTrials + i + ".bson");
				
				job.waitForCompletion(true);
				
				double totalResidual = ((double) job.getCounters().
						findCounter(RanPageRank.RanCounters.RESIDUAL).getValue()) / 10E6;
				double avgResidual = totalResidual / numNodes;
				
				sb.append("Iteration ").append(i).append(" has total residual ")
					.append(totalResidual).append(" with avg residual ")
					.append(avgResidual).append("\n");
				
				if (totalResidual < threshold) {
					break;
				}
			}
		} catch (Exception e) {
			System.out.println(sb.toString());
		}
		
		System.out.println(sb.toString());
		return 0;
	}

	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new RanPageRank(), args));
	}
}
