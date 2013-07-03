// d3display.js
// display data in a US map

var departuredelays = 
{
    	"TX" : 6786525,
	"CA" : 6513938,
	"FL" : 3680370,
	"IL" : 5140686,
	"GA" : 2969293,
	"NY" : 2385994,
	"CO" : 2663272,
	"NC" : 1298655,
	"AZ" : 1020870,
	"VA" : 1781863,
	"NV" : 1363293,
	"MI" : 973830,
	"NJ" : 1935447,
	"PA" : 811965,
	"MN" : 606580,
	"MO" : 944958,
	"WA" : 535238,
	"UT" : 415520,
	"TN" : 771606,
	"MA" : 799850,
	"MD" : 1144464,
	"OH" : 753912,
	"LA" : 474952,
	"OR" : 390735,
	"WI" : 398908,
	"KY" : 434936,
	"OK" : 262502,
	"IN" : 276727,
	"AL" : 272551,
	"HI" : 276413,
	"SC" : 282296,
	"NM" : 219888,
	"AR" : 261027,
	"PR" : 204269,
	"NE" : 165850,
	"CT" : 137931,
	"IA" : 164506,
	"ID" : 59616,
	"MT" : 9976,
	"MS" : 110528,
	"ND" : 83631,
	"RI" : 87408,
	"AK" : 36162,
	"KS" : 71244,
	"SD" : 67319,
	"WY" : 22268,
	"NH" : 88895,
	"ME" : 62748,
	"VI" : 18791,
	"VT" : 53308,
	"WV" : 39965,
	"TT" : 7502
};

var arrivaldelays = {
"CA" : 3737117,
	"TX" : 2762120,
	"IL" : 1964554,
	"FL" : 1893670,
	"NJ" : 1447281,
	"NY" : 1443279,
	"GA" : 1035868,
	"VA" : 989368,
	"CO" : 979413,
	"OH" : 531748,
	"MD" : 518827,
	"MO" : 506201,
	"PA" : 491325,
	"TN" : 426862,
	"NC" : 385862,
	"MA" : 358904,
	"HI" : 344777,
	"OK" : 316751,
	"WI" : 260350,
	"LA" : 255481,
	"NV" : 246558,
	"SC" : 221803,
	"KY" : 216505,
	"AL" : 214547,
	"AR" : 204809,
	"MI" : 198400,
	"IN" : 191237,
	"NE" : 160407,
	"OR" : 146431,
	"IA" : 144261,
	"NM" : 141999,
	"CT" : 131160,
	"PR" : 123767,
	"KS" : 86047,
	"NH" : 83396,
	"RI" : 75380,
	"MS" : 74355,
	"SD" : 72604,
	"ND" : 71166,
	"ID" : 59405,
	"ME" : 43875,
	"VT" : 42802,
	"WY" : 32094,
	"MT" : 22455,
	"VI" : 20781,
	"WV" : 20646,
	"WA" : 11142,
	"TT" : 7416,
	"UT" : -4940,
	"AK" : -17646,
	"AZ" : -40288,
	"MN" : -65302    
};

var idtoabbrev = JSON.parse('{"10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY","72":"PR","1":"AL","2":"AK","4":"AZ","5":"AR", "6":"CA","8":"CO","9":"CT"}');

var width = 960
, height = 500
, centered;

var mincrange = 1
, maxcrange = 1000;

// put "Departure Delay times for each state"
d3.select("body").append("h2").text("Departure Delay times for each State");
d3.select("body").append("p").text("Click state to zoom in and zoom out!");
mapdata(departuredelays);

// put "Arrival Delay times for each state"
d3.select("body").append("h2").text("Arrival Delay times for each state");
d3.select("body").append("p").text("Click state to zoom in and zoom out");
mapdata(arrivaldelays);

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
	.range(["yellow", "red"]);
    
    var path = d3.geo.path()
	.projection(projection);
    
    svg.append("rect")
	.attr("class", "background")
	.attr("width", width)
	.attr("height", height)
    .on("click", clicked);

    var g = svg.append("g");
    // g.attr("transform", "scale(.6 .6)");
    d3.json("/us-states.json", function(error, us) {
		var scolor = null;
		g.append("g")
		    .attr("id", "states")
		    .selectAll("path")
		    .data(topojson.feature(us, us.objects.states).features)
		    .enter().append("path")
		    .attr("d", path)
			.on("click", clicked)
			.on("mouseover", function() {
					scolor = d3.select(this).style("fill");
					d3.select(this).style("fill", "#1111ff");
				})
			.on("mouseout", function() {
					d3.select(this).style("fill", scolor);
					})
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
