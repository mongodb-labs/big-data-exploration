package nodeadends;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class NoDeadEndsReducer 
extends Reducer<Text, BSONWritable, Text, BSONWritable>{
	
	public void reduce( final Text id, 
			final Iterable<BSONWritable> values,
			final Context context ) 
					throws IOException, InterruptedException {

		ArrayList<String> links = new ArrayList<String>();
		int numLinks = 0;

		// Aggregate all the values for this key
		for (BSONWritable v : values) {
			BasicBSONObject o = (BasicBSONObject) v.getDoc();
			links.addAll((ArrayList<String>) o.get("links"));
			numLinks += (Integer) o.get("size");
		}

		BasicBSONObject lastly = new BasicBSONObject("_id", id.toString()).
				append("links", links).
				append("pg", 1.0 / pre.PreFormat.totalNodes).
				append("ptr", 1.0 / numLinks);

		context.write(id, new BSONWritable(lastly));
	}
}
