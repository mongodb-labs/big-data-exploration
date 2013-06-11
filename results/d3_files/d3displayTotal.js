// d3display.js
// display data in a US map

// flights out of each state
var flightsout = {
        "TX" : 495995,
	"CA" : 473998,
	"FL" : 410135,
	"IL" : 396823,
	"GA" : 389651,
	"NY" : 256227,
	"CO" : 236565,
	"NC" : 193787,
	"AZ" : 191673,
	"VA" : 183150,
	"NV" : 153315,
	"MI" : 125643,
	"NJ" : 121619,
	"PA" : 120090,
	"MN" : 116593,
	"MO" : 112427,
	"WA" : 111340,
	"UT" : 108090,
	"TN" : 106150,
	"MA" : 103423,
	"MD" : 102821,
	"OH" : 96014,
	"LA" : 70121,
	"OR" : 61097,
	"WI" : 59602,
	"KY" : 54138,
	"OK" : 43218,
	"IN" : 41198,
	"AL" : 36154,
	"HI" : 35059,
	"SC" : 33611,
	"NM" : 32793,
	"AR" : 31565,
	"PR" : 28344,
	"NE" : 23393,
	"CT" : 21434,
	"IA" : 20286,
	"ID" : 18233,
	"MT" : 16945,
	"MS" : 15743,
	"ND" : 14360,
	"RI" : 14220,
	"AK" : 14160,
	"KS" : 12661,
	"SD" : 11857,
	"WY" : 9878,
	"NH" : 9624,
	"ME" : 7049,
	"VI" : 4623,
	"VT" : 4367,
	"WV" : 3912,
	"TT" : 390
};

var flightsin =
{
        "TX" : 496191,
	"CA" : 474027,
	"FL" : 410161,
	"IL" : 396812,
	"GA" : 389669,
	"NY" : 256216,
	"CO" : 236457,
	"NC" : 193779,
	"AZ" : 191688,
	"VA" : 183189,
	"NV" : 153310,
	"MI" : 125600,
	"NJ" : 122304,
	"PA" : 120075,
	"MN" : 116604,
	"MO" : 112414,
	"WA" : 111342,
	"UT" : 108082,
	"TN" : 106149,
	"MA" : 103428,
	"MD" : 102824,
	"OH" : 96004,
	"LA" : 70115,
	"OR" : 61092,
	"WI" : 59580,
	"KY" : 54128,
	"OK" : 43224,
	"IN" : 41202,
	"AL" : 36151,
	"HI" : 35063,
	"SC" : 33629,
	"NM" : 32791,
	"AR" : 31571,
	"PR" : 27547,
	"NE" : 23395,
	"CT" : 21433,
	"IA" : 20294,
	"ID" : 18229,
	"MT" : 16946,
	"MS" : 15737,
	"ND" : 14362,
	"RI" : 14223,
	"AK" : 14161,
	"KS" : 12662,
	"SD" : 11853,
	"WY" : 9880,
	"NH" : 9624,
	"ME" : 7047,
	"VI" : 4622,
	"VT" : 4368,
	"WV" : 3920,
	"TT" : 390
};

var idtoabbrev = JSON.parse('{"10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY","72":"PR","1":"AL","2":"AK","4":"AZ","5":"AR", "6":"CA","8":"CO","9":"CT"}');

var width = 960
, height = 500
, centered;

var mincrange = 1
, maxcrange = 1000;

mapdata(flightsout);
mapdata(flightsin);

function mapdata(data) {    
    var svg = d3.select("body").append("svg")
	.attr("width", width)
	.attr("height", height);
    
    var projection = d3.geo.albersUsa()
	.scale(1070)
	.translate([width / 2, height / 2]);

    var scaleddata = scale(data, mincrange, maxcrange);

    var color = d3.scale.linear()
	.domain([mincrange, maxcrange])
	.range(["yellow", "green"]);
    
    var path = d3.geo.path()
	.projection(projection);
    
    svg.append("rect")
	.attr("class", "background")
	.attr("width", width)
	.attr("height", height);
    // .on("click", clicked);
    console.log("eyo!");
    var g = svg.append("g");
    // g.attr("transform", "scale(.6 .6)");
    d3.json("./d3_files/us-states.json", function(error, us) {
		g.append("g")
		    .attr("id", "states")
		    .selectAll("path")
		    .data(topojson.feature(us, us.objects.states).features)
		    .enter().append("path")
		    .attr("d", path)
		// .on("click", clicked)
		    .style("fill", function(d, i) {
			       var a = idtoabbrev[d.id];
			       if (a in scaleddata) {
				   return color(scaleddata[a]);
			       } 
			       return 0;
			   });
		
		g.append("path")
		    .datum(topojson.mesh(us, us.objects.states, function(a, b) { return a !== b; }))
		    .attr("id", "state-borders")
		    .attr("d", path);
	    });
}

// on-click event to zoom into a state
function clicked(d) {
  var x, y, k;

  if (d && centered !== d) {
    var centroid = path.centroid(d);
    x = centroid[0];
    y = centroid[1];
    k = 4;
    centered = d;
  } else {
    x = width / 2;
    y = height / 2;
    k = 1;
    centered = null;
  }

  g.selectAll("path")
      .classed("active", centered && function(d) { return d === centered; });

  g.transition()
      .duration(750)
      .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")scale(" + k + ")translate(" + -x + "," + -y + ")")
      .style("stroke-width", 1.5 / k + "px");
}


function getvalues(data) {
    vals = [];
    for (var k in data) {
	vals.push(data[k]);
    }
    return vals;
}

// scale to between min, max
// data must be all numbers
function scale(data, minr, maxr) {
    var mind = d3.min(getvalues(data))
    , maxd = d3.max(getvalues(data));
    
    var nscale = d3.scale.linear()
	.domain([mind, maxd])
	.range([minr, maxr]);

    var newdata = {};
    
    for (var k in data) {
	newdata[k] = nscale(data[k]);
    }
    return newdata;
}
