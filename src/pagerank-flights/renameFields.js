// Renames the fields to correspond with "value"
// Deprecated now that the transitionmatrix.py outputs "value"

function renameFields() {
	// For every node, emit this pagerank for the next iteration
	// coming from this node to 	all connected nodes
	var map = function() {
		emit(this["_id"], {"pg" : this.pg, "prs" : this.prs});
	};

	var reduce = function(airportId, probs) {
		return probs;
	};

	db.fpg_0.mapReduce(map, reduce, {out : "fpg_1"});
	db.fpg_0.drop();
	db.fpg_1.renameCollection("fpg_0");
}


renameFields();

