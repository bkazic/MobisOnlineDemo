
var count = 0;
var sumErr = 0;
function simpleMeanError(err) {
	count++;
	sumErr += err;
	var mean = sumErr/count;
	return mean;
}

exports.onlineMeanError = function(err) {
	return simpleMeanError(err);
};

exports.meanError = function(testSet, compareSet) {
	var meanError = -1;
	for (var ii=0; ii<testSet.length; ii++) {
		var rec = testSet[ii];
		var comparable = compareSet;
		comparable.filterByField("DateTime", rec.DateTime.string);
		if (!comparable) { count++; continue; }
		if (comparable.length===0) {count++; continue; }
		var pred = comparable[0].Prediction;
		var diff = Math.round(Math.abs(rec.Speed - pred) * 1000) / 1000;
		meanError = simpleMeanError(diff);
		//console.say("Mean error: " + meanError);
	}
	return meanError;
};

// About this module
exports.about = function() {
	var description = "Module with statistics functions for evaluation.";
	return description;
};