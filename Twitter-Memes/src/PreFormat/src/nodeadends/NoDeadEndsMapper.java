package nodeadends;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class NoDeadEndsMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {

    public void map(final Object obj,
            final BSONObject doc, 
            final Context context) 
                    throws IOException, InterruptedException {

        String url = ((String) doc.get("url")).replace("http://", "");
        ArrayList<BasicBSONObject> links = (ArrayList<BasicBSONObject>) doc.get("links");
        HashSet<String> unique = new HashSet<String>();

        // Create a list of destinations
        for (BasicBSONObject o : links) {
            String l = (String) o.get("single");
            unique.add(l.replace("http://", ""));
        }

        BSONObject out = new BasicBSONObject("_id", url)
                                .append("links", new ArrayList<String> (unique))
                                .append("size", unique.size());

        context.write(new Text(url), new BSONWritable(out));
    }

}
