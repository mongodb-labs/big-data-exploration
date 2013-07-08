package pre;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class PreFormatMapper 
		extends Mapper<Object, BSONObject, Text, BSONWritable> {

	public void map(final Object obj,
					final BSONObject doc, 
					final Context context) 
			throws IOException, InterruptedException {
		
		String url = (String) doc.get("url");
		ArrayList<BSONObject> links = (ArrayList<BSONObject>) doc.get("links");
		
		HashSet<String> unique = new HashSet<String>();
		ArrayList<String> to = new ArrayList<String>();
		
		for (BSONObject bbo : links) {
			String s = (String) bbo.get("single");
			if (unique.add(s)) {
				to.add(s);
			}
		}
		
		BSONObject out = new BasicBSONObject("links", to)
			.append("_id", url);
		
		context.write(new Text(url), new BSONWritable(out));
	}
	
}
