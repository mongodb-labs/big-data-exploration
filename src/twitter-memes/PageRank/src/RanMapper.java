import java.io.IOException;
import java.util.ArrayList;

import com.mongodb.hadoop.io.BSONWritable;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import org.bson.*;


public class RanMapper 
		extends Mapper<Object, BSONObject, Text, BSONWritable> {
	
	public RanMapper() {};
	
	protected void map(	final Object key, 
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
		BasicBSONObject self = new BasicBSONObject("ptr", value.get("ptr"));
		self.append("pg", 0).
			append("links", value.get("links")).
			append("prevpg", value.get("pg"));
		
		context.write(id, new BSONWritable(self));
		
		BasicBSONObject toEach = new BasicBSONObject("ptr", 0);
		toEach.append("pg", (Double) value.get("ptr") * (Double)value.get("pg")).
			append("links", new ArrayList<String>()).
			append("prevpg", 0);
		
		// Pass the proportional pagerank out
		for (String s : (ArrayList<String>) value.get("links")) {
			context.write(new Text(s), new BSONWritable(toEach));
		}
	}
}