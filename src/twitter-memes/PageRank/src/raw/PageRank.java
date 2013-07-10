package raw;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool{
	
	public static double beta = 0.9;
	public static double threshold = 0.001;
	public static double totalNodes = 20000000.0;
	public static double distributedBeta = (1-beta)/totalNodes;
	
	public static enum RanCounters {DEAD_END_PG, RESIDUAL, TOTAL_NODES};
	
	public Job createJob(int iteration, String numTrials, String inputString, 
			String outputString, long prevDeadEndsPG) 
					throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ran-pagerank");
		int last = iteration - 1;
		
		job.setJarByClass(PageRank.class);
		job.setMapperClass(raw.RawMapper.class);
		job.setReducerClass(raw.RawReducer.class);

		job.setInputFormatClass(com.mongodb.hadoop.BSONFileInputFormat.class);
		job.setOutputFormatClass(com.mongodb.hadoop.BSONFileOutputFormat.class);

		job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setOutputValueClass(com.mongodb.hadoop.io.BSONWritable.class);
		
		// Input and output paths can be specified since they would change in each round
		if (iteration == 0) {
			FileInputFormat.addInputPath(job, new Path(inputString));
			job.getConfiguration().setLong("deadEndsPG", 0);
		} else {
			FileInputFormat.addInputPath(job, new Path(outputString + numTrials + last));
			job.getConfiguration().setLong("deadEndsPG", prevDeadEndsPG);
		}
		job.getConfiguration().set("mapred.output.dir", outputString + numTrials + iteration);
		return job;
	}
	
	
	public String oneIteration(String numTrials, String inputString, String outputString) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		StringBuilder sb = new StringBuilder();
		long prevDeadEndsPG = 0;
		
		int i = 0;
		
		while (true) {
			
			Job job = createJob(i, numTrials, inputString, 
					outputString, prevDeadEndsPG);
			
			// Only continue if the previous job is finished
			job.waitForCompletion(true);
			
			// Calculate residuals
			double totalResidual = ((double) job.getCounters().
					findCounter(PageRank.RanCounters.RESIDUAL).getValue()) / 10E4;
			double avgResidual = totalResidual / totalNodes;
			prevDeadEndsPG = job.getCounters().findCounter(PageRank.RanCounters.DEAD_END_PG).
					getValue();

			sb.append("Iteration ").append(i).append(" has total residual ")
				.append(totalResidual).append(" with avg residual ")
				.append(avgResidual).append("\n");
			
			if (totalResidual < threshold * totalNodes) {
				break;
			}
			i += 1;
			break; //TODO CHANGE
		}
		
		return sb.toString();
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		StringBuilder sb = new StringBuilder();
		String numTrials = arg0[0];
		String inputString = arg0[1];
		String outputString = arg0[2];
		
		try {
			sb.append(oneIteration(numTrials, inputString, outputString));
		} catch (Exception e) {
			sb.append("\n");
			
            // Print the stack trace
            StringWriter writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter( writer );
            e.printStackTrace( printWriter );
            printWriter.flush();
            sb.append(writer.toString());
			
			System.out.println(sb.toString());
		}
		
		System.out.println(sb.toString());
		return 0;
	}
	

	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new PageRank(), args));
	}
}
