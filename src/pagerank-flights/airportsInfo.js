// airportsInfo.js
// JavaScript code to get information about all airpots in the database

// return map of
// airportId -> {airportCode: "JFK", airportState: "New York", airportStateId: "NY"
//              , airportCity: "New York, NY"}
var getAirportsInfo = function() {
    var airportInfo = {}
    , n = 40; // last iteration of PageRank
    
    var airportNodes = db["fpg_"+n].find()
    , flightInfo, id, airportNode;

    while (airportNodes.hasNext()) {
	airportNode = airportNodes.next();
	id = airportNode["_id"];
	airportInfo[id] = {"pg" : airportNode.value.pg};
	flightInfo = db.flights.findOne({"origAirportId" : parseInt(id)});

	airportInfo[id]["airportCode"] = flightInfo["origAirport"];
	airportInfo[id]["airportState"] = flightInfo["origState"];
	airportInfo[id]["airportStateId"] = flightInfo["origStateId"];
	airportInfo[id]["airportCity"] = flightInfo["origCity"];
    }

    return airportInfo;
};

var info = getAirportsInfo();
print(JSON.stringify(info));


