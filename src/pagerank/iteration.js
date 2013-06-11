// MapReduce for Simple PageRank


/* Map and Reduce functions defined for each iteration
 */
function oneiteration(n) {
    var map = function() {
        // For each node that is reachable from this node, give it the 
        // appropriate portion of my pagerank
	for (var toNode in this["value"]["prs"]) {
	    emit(toNode, {nextpg : this["value"]["prs"][toNode] * this["value"]["pg"]});
	}
	
        // Pass the previous pagerank and the probability matrix to myself
	emit(this["_id"], {prs : this["value"]["prs"], prevpg: this["value"]["pg"]});
	
        // TODO AN EXTRA PLACEHOLDER
        // An extra emit to myself so that the reducer is called
        // emit(this["_id"], {prs : this["value"]["prs"], prevpg: this["value"]["pg"]});
    };
    
    var reduce = function(airportId, values) {
	var pg = 0
        , diff = 0
	, prs = null
	, prevpg = 0 
	, beta = 0.9
        , all = 318;
        // TODO: This needs to be changed to the correct number of airports
        // Which remember, cannot be computed at reduce time. Hard Coded Only
	
	for (var i in values) {
	    // Retrieve the previous pagerank and the probability matrix
	    if ("prs" in values[i]) {
		prs = values[i]["prs"];
		prevpg = values[i]["prevpg"];
	    } else { 
                // Summation of the pagerank
		pg += values[i]["nextpg"];
	    }
	}
	
        pg = beta * pg + (1-beta) / all;
	diff = Math.abs(prevpg - pg) / prevpg;
	return {"pg" : pg, "prs" : prs, "diff": diff, "prevpg" : prevpg};
    };
    
    db["fpg_"+n].mapReduce(map, reduce, {out : "fpg_"+(n+1)});
}

/* Starting mapreduce with ending requirements
 */
function pagerank(eps) {
    var n = 0
    , totalDiff = 0;
    
    
    do {
        oneiteration(n);
        n += 1;
	
        // :::TODO:::
        // This query can be changed into map reduce, but it's a placeholder
        res = db["fpg_"+n].aggregate(
            {"$group" : {"_id" : 1, "totalDiff" : {"$sum" : "$value.diff"}}}
        );
	
        // Get the totalDiff (only 1 aggregation result)
        totalDiff = res["result"][0]["totalDiff"];
        print("totalDiff for iteration "+(n-1)+" is "+totalDiff);
    } while ( totalDiff > eps);
    
    return n - 1;
}

n = pagerank(0.001);
print("converged after "+n+" iterations");
