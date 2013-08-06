/* pagesstrategy.js ->
 *   Includes functions to build data structures to make searching
 *   for the presence of urls in the database faster.
 *   Called "pagestrategy" because it builds "pages" that contains
 *   all urls that share the first n characters.
 */

/*
 * Returns all possible combinations of keys of length n. Only 
 * characters allowed in url sub-domains are included.
 */
var lenNKeys = function(n) {
    if (n == 0) 
	return [""];

    var	keys = [];
    var oldkeys = lenNKeys(n-1);
    
    // insert a-z after oldkeys
    for (var i = 0; i < 26; i++)  {
	for (var j in oldkeys) {
	    keys.push(oldkeys[j] + String.fromCharCode(97+i));
	}
    }
    
    // insert . after oldkeys
    for (var j in oldkeys) {
	keys.push(oldkeys[j] + ".");
    }
    
    // insert _ after oldkeys
    for (j in oldkeys) {
	keys.push(oldkeys[j] + "_");
    }
    
    return keys;
};

/*
 * Builds a collection where each document has _id of length n that
 * indexes all the urls that are prefixed by the first n characters in 
 * the url (after http:// if any).
 * Doesn't assume that build(N-1)CharCollection was already called.
 */
var buildNCharCollection = function(n) {
    db["char"+n].drop();
    var newcoll = db["char"+n];
    
    var	docs = db.memes2.find().sort({_id : 1});
    var doc = docs.next();
    
    var url = doc["_id"]
    , prefix = null // first n letters of the current doc.value.url
    , c = 0
    , lastPrefix = (url.substring(0,4) == "http" ? 
		    url.substring(7, n) :
		    url.substring(0, n))
    , bulkData = [url];
    
    while (docs.hasNext()) {
        doc = docs.next();
	url = doc["_id"];

	prefix = (url.substring(0,4) == "http" ? 
		  url.substring(7, n) :
		  url.substring(0, n));
	
	if (prefix != lastPrefix) {
	    newcoll.insert({_id : lastPrefix, urls : bulkData});
	    bulkData = [];
            lastPrefix = prefix;
	} else {
	    bulkData.push(url);
	}
	
	c += 1;
	
	if (c % 1000 == 0) {
	    print("Put " + c + " documents so far");
	}
    }
};

/*
 * Builds a collection where each document has _id of length n that
 * indexes all the urls that are prefixed by the first n characters in 
 * the url (after http:// if any).
 * Assumes that build(N-1)CharCollection was already called.
 */
var buildNextCharCollection = function(n) {
    db["char"+(n+1)].drop();
    
    var oldcoll = db["char"+n]
    , newcoll = db["char"+(n+1)];
    
    var oldkeys = oldcoll.distinct("_id");
    
	// using old keys, build new key = old key + "a-z" or "."
    for (var i in oldkeys) {
	for (var j = 0; j < 26; j++) {
	    newcoll.insert({_id : oldkeys[i] + String.fromCharCode(97+j)});	
	}
	newcoll.insert({_id : oldkeys[i] + "."});
    }
    
    var nextL = null // get next letter "in line"
    , c = 0
    , doc = null;
    
    var docs = oldcoll.find();
    
    while (docs.hasNext()) {
	doc = docs.next();
	
	// go through every url in doc
	for (i in doc.urls) {
	    // get next letter 
	    nextL = doc.urls[i][n];
	    newcoll.update({_id : doc["_id"] + nextL}
			    , {$push : { 
				 urls : doc.urls[i]
			       }
			    });

	    c += 1;
	    if (c % 1000 == 0) {
		print("Put " + c + " documents so far");
	    }
	}
    }
};

// Used to build collection to reduce the set search space of all urls
// to only urls that share the first 8 characters
// buildNCharCollection(8);

// If need be, you can build upon the "8Char" collection 
// to build the "9Char" collection, the "10Char" collection, and so on
// buildNextCharCollection(8);


