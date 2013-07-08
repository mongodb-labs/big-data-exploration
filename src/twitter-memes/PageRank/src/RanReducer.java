import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.*;

import com.mongodb.hadoop.io.BSONWritable;


public class RanReducer extends 
		Reducer<Text, BSONWritable, Text, BSONObject>{
	
	public RanReducer() {};
	
	protected void reduce(Text key, java.lang.Iterable<BSONWritable> values, 
						Context context) 
			throws IOException, InterruptedException {
		
		double pg = 0;
		ArrayList<String> ls = new ArrayList<String>();
		double prevpg = 0;
		
		// Sum the pagerank
		for (BSONWritable v : values) {
			BasicBSONObject nested = (BasicBSONObject) v.getDoc();
			pg += nested.getDouble("pg");
			ls.addAll((ArrayList<String>) nested.get("links"));
			prevpg += nested.getDouble("prevpg");
		}
		
		// Include the random surfer
		pg = pg * RanPageRank.beta + RanPageRank.distributedBeta;
		double residual = Math.abs(prevpg - pg) / prevpg;
		
		// Need this 10E6 because long only takes whole numbers
		context.getCounter(RanPageRank.RanCounters.RESIDUAL).increment((long) (residual*10E6));
		
		BasicBSONObject out = new BasicBSONObject();
		out.append("ptr", 1.0 / ls.size()).
			append("pg", pg).
			append("links", ls).
			append("prevpg", prevpg);
			
		context.write(key, out);
	}
}
