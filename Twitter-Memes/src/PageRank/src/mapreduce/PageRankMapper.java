package mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;


public class PageRankMapper 
extends Mapper<Object, BSONObject, Text, BSONWritable> {


    /**
     * 
     * Add current pagerank to appropriate counters for debugging and 
     * deadEndsPG for next round to distribute
     * returns the pagerank for this round
     */
    protected double setCounters(double currentPG, BSONObject value, Context context) {
        context.getCounter(PageRank.RanCounters.BEFORE_PG)
        .increment((long) (currentPG * PageRank.counterFactor));

        // Get all the pagerank from last round in dead-ends, so disappeared
        long prevDeadEndsPGLONG = 0;
        String deadEndsString = context.getConfiguration().get("deadEndsPG");
        if (!(deadEndsString == null || deadEndsString.length() == 0)) {
            prevDeadEndsPGLONG = Long.parseLong(deadEndsString);
        }
        double prevDeadEndsPG = ((double) prevDeadEndsPGLONG) / PageRank.counterFactor;
        prevDeadEndsPG = prevDeadEndsPG / PageRank.totalNodes;

        // Distribute that dead end to everybody equally and use taxation parameter
        if (prevDeadEndsPG != 0.0) {
            currentPG = PageRank.beta * (currentPG + prevDeadEndsPG) + PageRank.distributedBeta;
        }
        double residual = 1;

        // Calculate the residual change if there was a previous round
        if (value.containsField("prevpg")) {
            double prevpg = (Double) value.get("prevpg");
            if (prevpg != 0) {
                residual = Math.abs(prevpg - currentPG) / prevpg;
            }
        }

        // Counters only take LONG
        context.getCounter(PageRank.RanCounters.RESIDUAL).increment((long) (residual * PageRank.counterFactor));
        context.getCounter(PageRank.RanCounters.TOTAL_PG).increment((long) (currentPG * PageRank.counterFactor));

        return currentPG;
    }

    protected void map(    
            final Object key, 
            final BSONObject value, 
            final Context context) 
                    throws IOException, InterruptedException {

        Text id = new Text((String) value.get("_id"));

        double currentPG = (Double) value.get("pg");
        // Map now calculates the pagerank values and sets counters
        currentPG = setCounters(currentPG, value, context);
        /*
         * The value format should be:
         * pg : passing pagerank to the next iteration
         * ls : probability matrix to the next iteration
         * prevpg : the pagerank of the previous round
         */

        // Passing the object back to myself for prev comparison
        BasicBSONObject self = new BasicBSONObject("pg", 0.0)
        .append("links", value.get("links"))
        .append("prevpg", currentPG);

        // Pass my probability matrix and pgs to myself
        context.write(id, new BSONWritable(self));

        BasicBSONObject links = (BasicBSONObject) value.get("links");
        if (links.isEmpty()) {
            // If this is a dead end then pass the pagerank to be given to all
            context.getCounter(PageRank.RanCounters.DEAD_END_PG)
            .increment((long) (currentPG * PageRank.counterFactor));
        } else {
            // Pass the proportional pagerank out
            BasicBSONObject toEach = new BasicBSONObject("links", new BasicBSONObject()).
                    append("prevpg", 0.0);
            for (String s : links.keySet()) {
                double prob = (double) links.getDouble(s);
                toEach.put("pg", currentPG * prob);
                context.write(new Text(s), new BSONWritable(toEach));
            }
        }
    }
}