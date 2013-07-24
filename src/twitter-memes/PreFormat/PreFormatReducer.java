package pre;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class PreFormatReducer 
		extends Reducer<Text, BSONWritable, Text, BSONWritable>{

	public void reduce( final Text id, 
						final Iterable<BSONWritable> values,
						final Context context ) 
			throws IOException, InterruptedException {
		
		ArrayList<String> links = new ArrayList<String>();
		int numLinks = 0;
		
		for (BSONWritable v : values) {
			BSONObject o = v.getDoc();
			ArrayList<String> thisLinks = (ArrayList<String>) o.get("links");
			numLinks += thisLinks.size();
			links.addAll(thisLinks);
		}
		
		double pg = 1.0 / PreFormat.TOTAL_NODES;
		
		BSONObject lastly = new BasicBSONObject();
		lastly.put("_id", id.toString());
		lastly.put("links", links);
		lastly.put("ptr", 1.0 / numLinks);
		lastly.put("pg", pg);
		
		context.write(id, new BSONWritable(lastly));
	}
}
