package raw;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class RawMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public void map(final Object obj,
			final BSONObject doc, 
			final Context context) 
					throws IOException, InterruptedException {


		String url = ((String) doc.get("url")).replace("http://", "");
		ArrayList<String> links = (ArrayList<String>) doc.get("links");
		BasicBSONObject to = new BasicBSONObject();

		// Create the frequency matrix of how many jumps to another doc
		for (String l : links) {
			l = l.replace("http://", "");
			if (to.get(l) != null) {
				to.put(l, ((Double) to.get(l)) + 1.0);
			} else {
				to.put(l, 1.0);
			}
		}

		BSONObject out = new BasicBSONObject("_id", url).
				append("links", to).
				append("size", links.size());

		context.write(new Text(url), new BSONWritable(out));
	}

}
