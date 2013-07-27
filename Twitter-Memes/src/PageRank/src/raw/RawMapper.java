package raw;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class RawMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {

    protected double setCounters(double currentPG, BSONObject value, Context context) {
        long deadEndsPG = 0;
        context.getConfiguration().getLong("deadEndsPG", deadEndsPG);
        double distributeDeadEndsPG = ((((double) deadEndsPG) / PageRank.deadEndsFactor) * PageRank.totalNodes); 
        currentPG = PageRank.beta * (currentPG + distributeDeadEndsPG) + PageRank.distributedBeta;
        double residual = 1;

        if (value.containsField("prevpg")) {
            double prevpg = (Double) value.get("prevpg");
            if (prevpg != 0) {
                residual = Math.abs(prevpg - currentPG) / prevpg;
            }
        }

        // Need this 10E1 because long only takes whole numbers
        context.getCounter(PageRank.RanCounters.RESIDUAL).increment((long) (residual*PageRank.residualFactor));

        return currentPG;
    }

    protected void map(    final Object key, 
            final BSONObject value, 
            final Context context) 
                    throws IOException, InterruptedException {

        Text id = new Text((String) value.get("_id"));

        double currentPG = (Double) value.get("pg");
        currentPG = setCounters(currentPG, value, context);
        /*
         * The value format should be:
         * pg : passing pagerank to the next iteration
         * ls : probability matrix to the next iteration
         * prevpg : the pagerank of the previous round
         */

        // Passing the object back to myself for prev comparison
        BasicBSONObject self = new BasicBSONObject("pg", 0)
        .append("links", value.get("links"))
        .append("prevpg", currentPG);

        context.write(id, new BSONWritable(self));

        BasicBSONObject links = (BasicBSONObject) value.get("links");
        if (links.size() == 0) {
            // If this is a dead end then pass the pagerank to be given to all
            context.getCounter(PageRank.RanCounters.DEAD_END_PG).
            increment((long) (currentPG * PageRank.deadEndsFactor));
        } else {
            // Pass the proportional pagerank out
            BasicBSONObject toEach = new BasicBSONObject("links", new BasicBSONObject()).
                    append("prevpg", 0);

            for (String s : links.keySet()) {
                double prob = (double) links.getDouble(s);
                toEach.put("pg", currentPG * prob);
                context.write(new Text(s), new BSONWritable(toEach));
            }
        }
    }
}
