package mapreduce;


import org.apache.hadoop.util.ToolRunner;
import com.mongodb.hadoop.util.MongoTool;

public class PreFormat extends MongoTool{
    
    public static void main(String[] args) throws Exception {
        
        System.exit(ToolRunner.run(new PreFormat(), args));
    }
}
