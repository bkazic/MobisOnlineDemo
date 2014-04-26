// Module for Loop counters preprocessing


exports.addNoDuplicateValues = function(outStore, rec) {
	var store = outStore; // Maybe I dont need this madafaka
	var val = rec.toJSON();
	var result = outStore.recs;

	result.filterByField("DateTime", rec.DateTime.string);
	if(typeof result != 'undefined') {
		if(result.hasOwnProperty("length")) { // checks if object has the specified property
			if(result.length > 0) {
				return; // do not add
			}
		}
	}

	delete val.$id; // when adding to QMiner, $id must not exist
	// add the join fields (different syntax)
	store.joins.forEach(function(join) {
		val[join] = {$id: rec[join].$id};
	});
	// add value
	store.add(val);
};

// If there is no cars, set speed to speed limit
exports.makeCleanSpeedNoCars = function(outStore) {
	var store = outStore;
	function cleanSpeedNoCars(rec) {
		if (rec.NumOfCars === 0) {
			speed = rec.measuredBy.MaxSpeed;
			store.add({$id:rec.$id, Speed:speed});
		}
	}
	return cleanSpeedNoCars;
};

// About this module
exports.about = function() {
	var description = "This module contains functions for prreprocessing counter loop sensors.";
	return description;
};