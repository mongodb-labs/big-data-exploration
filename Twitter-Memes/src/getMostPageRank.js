// Get the web pages with the most pagerank.
// Return the corresponding quotes associated 
// with these pages


var pagesWithMostPageRank = function(n, withHttp)  {
    var res = db.no_dead_ends_pg.find().sort({"pg" : -1}).limit(n)
    , page, o
    , results = [];

    
    // add in quotes associated with a web page
    while (res.hasNext()) {
	page = res.next();

	o = {};
	o.pg = page.pg;
	o.url = (withHttp ? page["_id"] : "http://" + page["_id"]);
	
	o.quotes = db.memes.findOne({"url" : o.url}).quotes;
	
	results.push(o);
    }

    return results;
};


print(JSON.stringify(pagesWithMostPageRank(10, false), undefined, 2));