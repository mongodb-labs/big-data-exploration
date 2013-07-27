// create a json that contains
var statestoabbrev = {
    Alabama: "AL"
    , Alaska : "AK"
    , Arizona : "AZ"
    , Arkansas : "AR"
    , California : "CA"
    , Colorado : "CO"
    , Connecticut : "CT"
    , Delaware : "DE"
    , Florida : "FL"
    , Georgia :	"GA"
    , Hawaii :	"HI"
    , Idaho : "ID"
    , Illinois: "IL"
    , Indiana : "IN"
    , Iowa : "IA"
    , Kansas : "KS"
    , Kentucky : "KY"
    , Louisiana : "LA"
    , Maine : "ME"
    , Maryland : "MD"
    , Massachusetts : "MA"
    , Michigan : "MI"
    , Minnesota : "MN"
    , Mississippi : "MS"
    , Missouri : "MO"
    , Montana :	"MT"
    , Nebraska : "NE"
    , Nevada : "NV"
    , "New Hampshire" :	"NH"
    ,  "New Jersey":	"NJ"
    , "New Mexico" : "NM"
    , "New York" : "NY"
    , "North Carolina" : "NC"
    , "North Dakota" :	"ND"
    , "Ohio" :	"OH"
    , "Oklahoma" : "OK"
    , "Oregon" : "OR"
    , Pennsylvania :	"PA"
    , "Rhode Island" :	"RI"
    , "South Carolina" : "SC"
    , "South Dakota" :	"SD"
    , "Tennessee" :	"TN"
    , Texas :	"TX"
    , "Utah" :	"UT"
    , Vermont :	"VT"
    , Virginia : "VA"
    , Washington : "WA"
    , "West Virginia" :	"WV"
    , Wisconsin:	"WI"
    , Wyoming :	"WY"
    , "American Samoa" : "AS"
    , "District of Columbia" :	"DC"
    ,    "Federated States of Micronesia" :	"FM"
    , "Guam" : "GU"
    , "Marshall Islands" :	"MH"
    , "Northern Mariana Islands" :	"MP"
    , "Palau" :	"PW"
    , "Puerto Rico" :	"PR"
    , "Virgin Islands" :	"VI"
}
// 
// id -> name mappings
var idtostates = {};
var idtoabbrev = {};
var abbrevtoid = {};

d3.json("us-states.json", function(json) {
	    features = json.features;
	    var i = 0;
	    for (var k in features) {
	       	idtostates[features[k].id] = features[k].properties.name;
		idtoabbrev[features[k].id] = statestoabbrev[idtostates[features[k].id]];
		abbrevtoid[idtostates[features[k].id]] = features[k].id;
		i++;
	    }
            //  console.log(JSON.stringify(idtostates));
	    console.log(JSON.stringify(idtostates));
	    console.log(JSON.stringify(abbrevtoid));
	    console.log(JSON.stringify(idtoabbrev));
	    console.log(i);
});


