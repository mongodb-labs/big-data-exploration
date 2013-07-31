package mapreduce;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

@SuppressWarnings("unchecked")
public class PreFormatMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {

    public void map(final Object obj,
            final BSONObject doc, 
            final Context context) 
                    throws IOException, InterruptedException {
        try {
            // Sending an edge serves to make sure that all nodes
            // in the graph start with an initial pg
            BasicBSONObject edge = new BasicBSONObject("size", 0)
            .append("links", new BasicBSONObject());

            String url = (String) doc.get("url");

            URI uri = new URI(url);

            ArrayList<Object> links = (ArrayList<Object>) doc.get("links");
            BasicBSONObject freqMatrix = new BasicBSONObject();

            // Create the frequency matrix of how many jumps to another doc
            for (Object l : links) {

                String destination = "";


                // This input didn't go through the erase dead ends
                if (links.get(0) instanceof String) {
                    destination = (String) l;
                } else {
                    destination = (String) ((BasicBSONObject) l).get("single");
                }

                // Get the absolute url
                if (!destination.startsWith("http") && !destination.startsWith("ftp")) {
                    destination = uri.resolve(destination).toString();
                }
                
                if (freqMatrix.get(destination) != null) {
                    freqMatrix.put(destination, ((Double) freqMatrix.get(destination)) + 1.0);
                } else {
                    freqMatrix.put(destination, 1.0);
                }
                edge.put("_id", destination);
                context.write(new Text(destination), new BSONWritable(edge));
            }


            BSONObject out = new BasicBSONObject("_id", url)
            .append("links", freqMatrix)
            .append("size", links.size());

            context.write(new Text(url), new BSONWritable(out));
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
