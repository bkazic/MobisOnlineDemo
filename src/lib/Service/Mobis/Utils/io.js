
//function printStr(vec) {
exports.printStr = function(vec) {
	var outStr =  "[";
	for (var ii=0; ii<vec.length-2; ii++) {
		outStr += vec.at(ii).toString()+", ";
	}
	outStr += vec.at(vec.length-1)+"]";
	return outStr;
};

exports.vecToStr = function(vec) {
	var str = "\n[\n";
	for (var rowN = 0; rowN < vec.length; rowN++) {
		str += vec.at(rowN).toFixed(2) + "\n";
	}
	str += "]\n";
	return str;
};

exports.saveMat = function(X, fname) {
	var Xstr = printStr(X);
	console.say(Xstr);
	var outFile = fs.openAppend("./sandbox/sensors/test/" + fname);
	outFile.writeLine(Xstr);
	outFile.flush();
};

// About this module
exports.about = function() {
	var description = "Module with input output functions";
	return description;
};