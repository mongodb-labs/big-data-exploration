package raw;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class RawReducer 
extends Reducer<Text, BSONWritable, Text, BSONWritable>{

	public void reduce( final Text id, 
			final Iterable<BSONWritable> values,
			final Context context ) 
					throws IOException, InterruptedException {

		BasicBSONObject links = new BasicBSONObject();
		int numLinks = 0;

		// Aggregate all the values for this key
		for (BSONWritable v : values) {
			BSONObject o = v.getDoc();
			BSONObject thisLinks = (BasicBSONObject) o.get("links");
			numLinks += (Integer) o.get("size");
			links.putAll(thisLinks);
		}

		BasicBSONObject finalObject = new BasicBSONObject();

		// Create a Probability Matrix
		for (String s: links.keySet()) {
			finalObject.append(s, ((Double) links.get(s)) / numLinks);
		}

		// Find the initial pagerank
		double pg = 1.0 / pre.PreFormat.totalNodes;

		BSONObject lastly = new BasicBSONObject().
				append("_id", id.toString()).
				append("links", finalObject).
				append("pg", pg);

		context.write(id, new BSONWritable(lastly));
	}
}
