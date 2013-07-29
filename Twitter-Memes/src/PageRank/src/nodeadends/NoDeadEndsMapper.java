package nodeadends;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class NoDeadEndsMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {

    public NoDeadEndsMapper() {};

    protected void map(    final Object key, 
            final BSONObject value, 
            final Context context) 
                    throws IOException, InterruptedException {

        Text id = new Text((String) value.get("_id"));

        /*
         * The value format should be:
         * ptr : 1 / num of outlinks
         * pg : passing pagerank to the next iteration
         * ls : links to the next iteration
         * prevpg : the pagerank of the previous round
         */

        // Passing the object back to myself for prev comparison
        BasicBSONObject self = new BasicBSONObject("ptr", value.get("ptr"))
                .append("pg", 0)
                .append("links", value.get("links"))
                .append("prevpg", value.get("pg"));

        context.write(id, new BSONWritable(self));

        BasicBSONObject toEach = new BasicBSONObject("ptr", 0)
                .append("pg", (Double) value.get("ptr") * (Double) value.get("pg"))
                .append("links", new ArrayList<String>())
                .append("prevpg", 0);

        // Pass the proportional pagerank out
        for (String s : (ArrayList<String>) value.get("links")) {
            context.write(new Text(s), new BSONWritable(toEach));
        }
    }
}