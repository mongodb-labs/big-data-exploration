package nodeadends;
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
    public static double numNodes = 1113524.0;
    public static double distributedPG = (1 - beta) / numNodes;
    
    public static enum RanCounters {DEAD_END_PG, RESIDUAL, TOTAL_NODES};
    
    @Override
    public int run(String[] arg0) throws Exception {
        StringBuilder sb ;
        String numTrials = arg0[0];
        String inputString = arg0[1];
        String outputString = arg0[2];
        
        try {
            Configuration conf;
            Job job;
            int i = 0;
            while (true) {
                int last = i - 1;
                
                // Because the job is iterative, must set all these in the program
                conf = new Configuration();
                job = new Job(conf, "ran-pagerank");
                
                job.setJarByClass(PageRank.class);
                job.setMapperClass(nodeadends.NoDeadEndsMapper.class);
                job.setReducerClass(nodeadends.NoDeadEndsReducer.class);
        
                job.setInputFormatClass(com.mongodb.hadoop.BSONFileInputFormat.class);
                job.setOutputFormatClass(com.mongodb.hadoop.BSONFileOutputFormat.class);
        
                job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
                job.setOutputValueClass(com.mongodb.hadoop.io.BSONWritable.class);
                
                // Input and output paths can be specified since they would change in each round

                if (i == 0) {
                    FileInputFormat.addInputPath(job, new Path(inputString));
                } else {
                    FileInputFormat.addInputPath(job, new Path(outputString + numTrials + last));
                }
                job.getConfiguration().set("mapred.output.dir", outputString + numTrials + i);
                
                // Only continue if the previous job is finished
                job.waitForCompletion(true);
                
                // Calculate residuals
                double totalResidual = ((double) job.getCounters().
                        findCounter(PageRank.RanCounters.RESIDUAL).getValue()) / 10E6;
                double avgResidual = totalResidual / numNodes;
                
                sb = new StringBuilder();
                sb.append("Iteration ").append(i).append(" has total residual ")
                    .append(totalResidual).append(" with avg residual ")
                    .append(avgResidual).append("\n");
                
                System.out.println(sb.toString());
                
                if (totalResidual < threshold * numNodes) {
                    break;
                }
                i += 1;
            }
        } catch (Exception e) {
            sb = new StringBuilder();
            sb.append("\n");
            
            // Print the stack trace
            StringWriter writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter( writer );
            e.printStackTrace( printWriter );
            printWriter.flush();
            sb.append(writer.toString());
            
            System.out.println(sb.toString());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new PageRank(), args));
    }
}
