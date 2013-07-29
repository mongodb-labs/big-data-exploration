// d3pagerankdisplay.js

// insert label
d3.select("body").append("p").text("PageRank of Commercial Airports in the U.S based on Domestic Flights Information");

var centered;

// returns the minimum PageRank
var getMinPageRank = function(airportPageRanks) {
    var minSoFar = 100;
    for (var k in airportPageRanks) {
       if (airportPageRanks[k].pg < minSoFar) {
          minSoFar = airportPageRanks[k].pg;
       }
    }
	return minSoFar;
};

// returns th for everything ending
// in 0, 4, 5, 6, 7, 8, 9
// returns rd for 3, returns nd for 2 
var formatposition = function(n) {
	var last = n % 10, lastf = null;
	
	if (last == 1) {
		lastf = "st";
	} else if (last == 2) {
		lastf = "nd";
	} else if (last == 3) {
		lastf = "rd";
	} else {
		lastf = "th";
	}

	var f = String(parseInt(n/10)),
	m = String(last),
	l = String(lastf);

	return (f === "0" ? "" : f) + m + l;
};


// store (airportCode, %pg)
var rankings = [];
// store airportCode => position in ranking (1st, etc)
var airportRankings = {};

var w = 1280,
    h = 800;

var projection = d3.geo.azimuthal()
    .mode("equidistant")
    .origin([-98, 38])
    .scale(1000)
    .translate([500, 350]);

var path = d3.geo.path()
    .projection(projection);

var svg = d3.select("body").insert("svg:svg", "h2")
    .attr("width", w)
    .attr("height", h);

var states = svg.append("svg:g")
    .attr("id", "states");

var circles = svg.append("svg:g")
    .attr("id", "circles");

var cells = svg.append("svg:g")
    .attr("id", "cells");

d3.json("us-states.json", function(collection) {
  states.selectAll("path")
      .data(collection.features)
    .enter().append("svg:path")
      .attr("d", path);
});

var minpg = getMinPageRank(airportPageRanks);

var pageRankByAirport = {};

var linksByOrigin = {},
countByAirport = {},
locationByAirport = {},
positions = [];

var arc = d3.geo.greatArc()
    .source(function(d) { return locationByAirport[d.source]; })
    .target(function(d) { return locationByAirport[d.target]; });

var apg;
for (var i in airportPageRanks) {
    apg = airportPageRanks[i];
    pageRankByAirport[apg.airportCode] = apg.pg;
	rankings.push([apg.airportCode, apg.pg]);
}

// sort rankings in descending order of pagerank
rankings.sort(function(a, b) {return b[1]-a[1];});

for (i = 0; i < rankings.length; i++) {
	airportRankings[rankings[i][0]] = i+1;
}

d3.csv("airports.csv", function(airports) {
		   // Only consider airports with at least one flight.
		   airports = airports.filter(function(airport) {
		if (airport.iata in pageRankByAirport) {
        var location = [+airport.longitude, +airport.latitude];
        locationByAirport[airport.iata] = location;
        positions.push(projection(location));
        return true;
      }
    });

    // Compute the Voronoi diagram of airports' projected positions.
    var polygons = d3.geom.voronoi(positions);

    var g = cells.selectAll("g")
        .data(airports)
      .enter().append("svg:g");

    g.append("svg:path")
        .attr("class", "cell")
		.attr("d", function(d, i) { return "M" + polygons[i].join("L") + "Z"; })
        .on("mouseover", function(d, i) { 
				if (d.iata in airportRankings) {
					d3.select("h2 span").text(d.name + "; " + d.iata + "; " + (pageRankByAirport[d.iata]*100).toFixed(2) + "%; " + formatposition(airportRankings[d.iata])); 	
				}
		});

	var scolor = null;
    circles.selectAll("circle")
        .data(airports)
      .enter().append("svg:circle")
        .attr("cx", function(d, i) { return positions[i][0]; })
        .attr("cy", function(d, i) { return positions[i][1]; })
        .attr("r", function(d, i) { return Math.sqrt((pageRankByAirport[d.iata]-minpg)*15000 + 10); })
		.style("fill", function(d, i) {
			return "#ff1122";
		})
        .sort(function(a, b) { return pageRankByAirport[b.iata] - pageRankByAirport[a.iata]; });
});



