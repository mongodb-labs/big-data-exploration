// MapReduce for Simple PageRank


/* Map and Reduce functions defined for each iteration
 */
function oneiteration(n) {
    var map = function() {
        // Pass the previous pagerank and the probability matrix to myself
        emit(this["_id"], 
             {"ptr" : this["value"]["ptr"]
                , "pg" : 0.0
                , "ls" : this["value"]["ls"]
                , "prevpg" : this["value"]["pg"] 
                , "diff" : 0.0}
            );

        // For each node that is reachable from this node, give it the 
        // appropriate portion of my pagerank
        for (var i = 0 ; i < this["value"]["ls"].length ; i++) {
            var toNode = this["value"]["ls"][i];
            var amount = this["value"]["ptr"] * this["value"]["pg"];

            emit(toNode, 
                    {"ptr" : 0.0 
                    , "pg" : amount 
                    , "ls" : []
                    , "prevpg" : 0.0
                    , "diff" : 0.0}
                );
        }
    };
        
    var reduce = function(idURL, values) {
        var ptr = 0.0
        , pg = 0.0
        , diff = 0
        , ls = []
        , prevpg = 0.0 
        , beta = 0.9
        , all = 102000; 
        // TODO: This needs to be changed to the correct number of airports
        // Which remember, cannot be computed at reduce time. Hard Coded Only
        
        for (var i in values) {
            // Summation of the pagerank
            pg += values[i]["pg"];

            // Retrieve the previous pagerank and the probability matrix
            ls = ls.concat(values[i]["ls"]);
            prevpg += values[i]["prevpg"];
            ptr += values[i]["ptr"];
        }
        
        pg = beta * pg + (1-beta) / all;
        diff = Math.abs(prevpg - pg) / prevpg;
        return {
            "ptr" : ptr
            , "pg" : pg 
            , "ls" : ls
            , "prevpg" : prevpg
            , "diff" : diff};
    };
    
    db["mpg_"+n].mapReduce(map, reduce, {out : "mpg_"+(n+1)});
}

/* Starting mapreduce with ending requirements
 */
function pagerank(eps) {
    var n = 0
    , totalDiff = 0;
    
    
    do {
        oneiteration(n);
        n += 1;
    
        // This query can be changed into map reduce, but it's a placeholder
        res = db["mpg_"+n].aggregate(
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
