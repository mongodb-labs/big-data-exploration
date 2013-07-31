package mapreduce;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool{
    
    public static double beta = 0.9;
    public static double threshold = 0.001;
    public static double totalNodes = 36814086.0;
    public static double distributedBeta = (1-beta)/totalNodes;
    public static double counterFactor = 10E11;
    
    public static enum RanCounters {DEAD_END_PG, RESIDUAL, TOTAL_PG, BEFORE_PG};
    
    public Job createJob(int iteration, String inputString, 
            String outputString, long deadEndsPG) 
                    throws IOException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ran-pagerank");
        int last = iteration - 1;
        
        job.setJarByClass(PageRank.class);
        job.setMapperClass(mapreduce.PageRankMapper.class);
        job.setReducerClass(mapreduce.PageRankReducer.class);

        job.setInputFormatClass(com.mongodb.hadoop.BSONFileInputFormat.class);
        job.setOutputFormatClass(com.mongodb.hadoop.BSONFileOutputFormat.class);

        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(com.mongodb.hadoop.io.BSONWritable.class);
        
        // Input and output paths can be specified since they would change in each round
        if (iteration == 0) {
            FileInputFormat.addInputPath(job, new Path(inputString));
            // Configuration would only take in a string, numbers wouldn't be returned
            job.getConfiguration().set("deadEndsPG", "0");
        } else {
            FileInputFormat.addInputPath(job, new Path(outputString + last));
            job.getConfiguration().set("deadEndsPG", "" + deadEndsPG);
        }
        job.getConfiguration().set("mapred.output.dir", outputString + iteration);
        return job;
    }
    
    
    /**
     * Creates a job to run the PageRank algorithm once 
     * with the inputString and outputString
     * checks to see if the residual is less than 0.001 * totalNodes
     */
    public void oneIteration(String inputString, String outputString) 
            throws IOException, InterruptedException, ClassNotFoundException {
        
        int i = 0;
        long prevDeadEndsPG = 0;
        
        while (true) {
            StringBuilder sb = new StringBuilder();
            
            Job job = createJob(i, inputString, outputString, prevDeadEndsPG);
            
            // Only continue if the previous job is finished
            job.waitForCompletion(true);
            
            // Calculate residuals
            double totalResidual = ((double) job.getCounters().
                    findCounter(PageRank.RanCounters.RESIDUAL).getValue())/ counterFactor;
            // Overflow
            if (totalResidual < 0) {
                totalResidual = totalNodes;
            }
            double avgResidual = totalResidual / totalNodes;
            prevDeadEndsPG = job.getCounters().findCounter(PageRank.RanCounters.DEAD_END_PG).
                    getValue();
            double totalPG = ((double) job.getCounters()
                    .findCounter(PageRank.RanCounters.TOTAL_PG).getValue()) / counterFactor;
            double beforePG = ((double) job.getCounters()
                    .findCounter(PageRank.RanCounters.BEFORE_PG).getValue()) / counterFactor;

            sb.append("Iteration ").append(i).append(" has:")
                .append("\n\t Total residual : ").append(totalResidual)
                .append("\n\t Avg residual : ").append(avgResidual)
                .append("\n\t PG before calculations : ").append(beforePG)
                .append("\n\t Total pg : ").append(totalPG)
                .append("\n\t Dead end pg : ").append(prevDeadEndsPG)
                .append("\n");
            
            System.out.print(sb.toString());
            
            if (totalResidual < threshold * totalNodes) {
                break;
            }
            i += 1;
        }
    }
    
    
    @Override
    // Because we have to run the iterations/jobs multiple times, the inputs come in
    // as strings instead of config values
    public int run(String[] arg0) throws Exception {
        StringBuilder sb = new StringBuilder();
        String inputString = arg0[0];
        String outputString = arg0[1];
        
        try {
            oneIteration(inputString, outputString);
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
