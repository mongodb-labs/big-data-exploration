// pagesstrategy.js
// build data structures to make searching for the presence of
// urls in the database faster

// return all possible keys of length n
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

var buildNCharCollection = function(n) {
    db["char"+n].drop();
    var newcoll = db["char"+n];
    
    var	docs = db.memes2.find().sort({_id : 1});
    
    var doc = docs.next();
    
    var url = doc["_id"]
    , prefix = null // first n letters of the current doc.value.url
    , c = 0
    , lastPrefix = doc["_id"].substring(0, n) // the last prefix
    , bulkData = [url];
    
    while (docs.hasNext()) {
        doc = docs.next();
	url = doc["_id"];
	prefix = url.substring(0, n);
	
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

var build1CharCollection = function() {
    db.char1.drop();
    
    var newcoll = db.char1;
    var doc = null;
    
    // insert 0-9
    for (var i = 0; i < 10; i++) {		
	doc = {_id : String(i), urls : []};
	newcoll.insert(doc);
    }
    
    // insert a-z
    for (i = 0; i < 26; i++) {
	doc = {_id : String.fromCharCode(97+i), urls : []};
	newcoll.insert(doc);
    }
    
    var	docs = db.memes.find();
    
    var urlToInsert = null
    , firstL = null // get first letter
    , c = 0;
    
    while (docs.hasNext()) {
	doc = docs.next();
	url = doc.url;
	
	if (url.substring(0, 4) == "http") {
	    // skip http://
	    urlToInsert = url.substring(7);	
	} else {
	    urlToInsert = url;
	}
	firstL = urlToInsert[0];
	
	newcoll.update({_id : firstL}
			, {$push : { 
			     urls : urlToInsert
			     }
			   }
		      	);
	c += 1;
	
	if (c % 1000 == 0) {
	    print("Put " + c + " documents so far");
	}
    }
};

// build char(n+1) collection based on char(n) collection
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
	    nextL = doc.urls[i][n];
	    
	    newcoll.update({_id : doc["_id"] + nextL}
						 , {$push : { 
								urls : doc.urls[i]
							}
						   }
						);
	    c += 1;
	    
	    if (c % 1000 == 0) {
		print("Put " + c + " documents so far");
	    }
	}
    }
};

// build1CharCollection();
// buildNextCharCollection(1);
buildNCharCollection(8);



// query to check if all have http://
// db.memes.find({ url: /^(http:).*/}).count()
// NOT ALL DO

