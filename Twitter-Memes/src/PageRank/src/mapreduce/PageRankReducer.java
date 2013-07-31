package mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class PageRankReducer extends 
        Reducer<Text, BSONWritable, Text, BSONObject>{
    
    protected void reduce(Text key, java.lang.Iterable<BSONWritable> values, 
                        Context context) 
            throws IOException, InterruptedException {
        
        double pg = 0;
        BasicBSONObject ls = new BasicBSONObject();
        double prevpg = 0;
        
        // Sum the pagerank
        for (BSONWritable v : values) {
            BasicBSONObject nested = (BasicBSONObject) v.getDoc();
            pg += (Double) nested.get("pg");
            ls.putAll((BSONObject) nested.get("links"));
            prevpg += (Double) nested.get("prevpg");
        }
        
        BasicBSONObject out = new BasicBSONObject();
        out.append("_id", key.toString())
            .append("pg", pg)
            .append("links", ls)
            .append("prevpg", prevpg);
            
        context.write(key, out);
    }
}
