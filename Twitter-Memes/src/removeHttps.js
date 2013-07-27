// removeHttps.js

// remove the http:// prefix from all urls that have such 
// prefix
var removehttps = function() {
    var map = function() {
	var doc = this;
	var url = doc["url"];
	
	if (url.substring(0, 4) == "http") {
	    url = url.substring(7);
	} else {
	    return;
	}
	
        delete doc["url"];
        delete doc["_id"];
	
	var link;
	
	for (var i in doc["links"]) {
	    link = doc["links"][i];
	    if (link.substring(0, 4) == "http") {
		link = link.substring(7);
	    } else {
		delete doc["links"][i];
		continue;
	    }
	    doc["links"][i] = link;
	}
	
	// check if this node doesn't point to any other node
        if (doc["links"].length == 0) return;
	
	emit(url, doc);
    };	
    
    var reduce = function(url, values) {		
        return values[0]
    };
    
    db.memes.mapReduce(map, reduce, {out: "memes2"});
};

removehttps();
