package pre;


import org.apache.hadoop.util.ToolRunner;
import com.mongodb.hadoop.util.MongoTool;

public class PreFormat extends MongoTool{

    // Hard set value, but would like this to be done in the program
    public final static double totalNodes = 1113522.0;
    
    public static void main(String[] args) throws Exception {
        
        System.exit(ToolRunner.run(new PreFormat(), args));
    }
}
