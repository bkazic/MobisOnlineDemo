var analytics = require('analytics');

//import "functions.j
var Service = {}; Service.Mobis = {}; Service.Mobis.Utils = {};
Service.Mobis.Utils.RidgeRegression = require('Service/Mobis/Utils/ridgeRegression.js');
Service.Mobis.Loop = require('Service/Mobis/Loop/preproc.js');
Service.Mobis.Utils.Stat = require('Service/Mobis/Utils/stat.js');
Service.Mobis.Utils.Io = require('Service/Mobis/Utils/io.js');


// Loading store for counter Nodes
var filename_counters_n = "./sandbox/sensors/countersNodes.txt";
var CounterNode = qm.store("CounterNode");
qm.load.jsonFile(CounterNode, filename_counters_n);

// Define measurement store definition as a function so that it can be used several times 
var measurementStoresDef = function (storeName, extraFields) {
   storeDef = [{
       "name" : storeName,
       "fields" : [
            { "name" : "DateTime", "type" : "datetime" },
            { "name" : "NumOfCars", "type" : "float", "null" : true },
            { "name" : "Gap", "type" : "float", "null" : true },
            { "name" : "Occupancy", "type" : "float", "null" : true },
            { "name" : "Speed", "type" : "float", "null" : true },
            { "name" : "TrafficStatus", "type" : "float", "null" : true }
        ],
        "joins" : [
            { "name" : "measuredBy", "type" : "field", "store" : "CounterNode" }
        ]
  }];
  if(extraFields) {
    storeDef[0].fields = storeDef[0].fields.concat(extraFields);
  }
  qm.createStore(storeDef);
};

// Find measurement files
var fileList = fs.listFile("./sandbox/sensors/","txt");
var keyWord = "measurements_"; // Keyword, by which to find files
var fileListMeasures = fileList.filter( function(element) {return element.indexOf(keyWord) >= 0;}); // Find files with keyword in name
var measurementIds = fileListMeasures.map( function(element) {return element.substring(element.length-12,element.length-4);}); // Extract IDs from file names

// Load measurement files to stores
for (var i=0; i<fileListMeasures.length; i++) {
  // Creating name for stores
  var storeName = "CounterMeasurement"+measurementIds[i];
  var storeNameClean = storeName + "_Cleaned";
  var storeNameResampled = storeName + "_Resampled";

  // Creating Store for measurements
  measurementStoresDef(storeName);
  measurementStoresDef(storeNameClean);
  measurementStoresDef(storeNameResampled, [{ "name" : "Ema1", "type" : "float", "null" : true },
                                            { "name" : "Ema2", "type" : "float", "null" : true },
                                            { "name" : "Prediction", "type" : "float", "null" : true},
                                            { "name" : "PredictionWas", "type" : "float", "null" : true},
                                            { "name" : "Difference", "type" : "float", "null" : true}]);

  // Load measurement files to created store
  var store = qm.store(storeName);
  qm.load.jsonFile(store, fileListMeasures[i]);
}

// Open the first two stores
var testStore = qm.store(qm.getStoreList()[1].storeName);
var testStoreClean = qm.store(qm.getStoreList()[2].storeName);
var testStoreResampled = qm.store(qm.getStoreList()[3].storeName);


// Here addNoDuplicateValues should be added later
testStore.addTrigger({
  onAdd: function (rec) {
	Service.Mobis.Loop.addNoDuplicateValues(testStoreClean, rec);
  }
});

//TODO: this is not used, because addStreamAggr is executed before this
// Calls function that cleanes speed when no cars are detected
testStoreClean.addTrigger({
  onAdd : Service.Mobis.Loop.makeCleanSpeedNoCars(testStoreClean)
});

// This resample aggregator creates new resampled store
testStoreClean.addStreamAggr({ name: "Resample1min", type: "resampler",
  outStore: testStoreResampled.name, timestamp: "DateTime",
  fields: [ { name: "NumOfCars", interpolator: "previous" },
            { name: "Gap", interpolator: "previous" },
            { name: "Occupancy", interpolator: "previous" },
            { name: "Speed", interpolator: "previous" },
            { name: "TrafficStatus", interpolator: "previous" } ],
  createStore: false, interval: 300*1000 });

// insert testStoreResampled store aggregates
testStoreResampled.addStreamAggr({ name: "tick", type: "timeSeriesTick",
                                   timestamp: "DateTime", value: "Speed" });
testStoreResampled.addStreamAggr({ name: "Ema1", type: "ema", inAggr: "tick",
                                  emaType: "previous", interval: 2000*1000, initWindow: 600*1000 });
testStoreResampled.addStreamAggr({ name: "Ema2", type: "ema", inAggr: "tick",
                                  emaType: "previous", interval: 10000*1000, initWindow: 600*1000 });


// Buffer defines for how many records infront prediction will be learned
testStoreResampled.addStreamAggr({ name: "delay", type: "recordBuffer", size: 6});

// Define feature space
var ftrSpace = analytics.newFeatureSpace([
  { type: "numeric", source: testStoreResampled.name, field: "Speed" },
  { type: "numeric", source: testStoreResampled.name, field: "Ema1" },
  { type: "numeric", source: testStoreResampled.name, field: "Ema2" },
  { type: "multinomial", source: testStoreResampled.name, field: "DateTime", datetime: true }
]);

// Initialize ridge regression. Input parameters: regularization factor, dimension, buffer.
console.say(Service.Mobis.Utils.RidgeRegression.about());
var ridgeRegression = new Service.Mobis.Utils.RidgeRegression.ridgeRegression(0.003, ftrSpace.dim, 100);

testStoreResampled.addTrigger({
  onAdd: function (rec) {

    var ema1 = testStoreResampled.getStreamAggr("Ema1").EMA;
    var ema2 = testStoreResampled.getStreamAggr("Ema2").EMA;
    testStoreResampled.add({ $id: rec.$id, Ema1: ema1, Ema2: ema2 });

    var prediction = ridgeRegression.predict(ftrSpace.ftrVec(rec));
    testStoreResampled.add({ $id: rec.$id, Prediction: prediction });

    var trainRecId = testStoreResampled.getStreamAggr("delay").first;

    if (trainRecId > 0) {
      ridgeRegression.addupdate(ftrSpace.ftrVec(testStoreResampled[trainRecId]), rec.Speed);
      console.log("Learning model.");

      // to get parameters from model
      // var model = ridgeRegression.getModel();
      // model.print();
      // var modelString = Service.Mobis.Utils.Io.printStr(model);

    }

    var predictionWas = testStoreResampled[trainRecId].Prediction;
    var diff = Math.round(Math.abs(rec.Speed - predictionWas) * 1000) / 1000;
    
    testStoreResampled.add({ $id: rec.$id, PredictionWas: predictionWas, Difference: diff });
    console.log("Diff: " + diff + ", Value: " + rec.Speed + ", Prediction: " + testStoreResampled[trainRecId].Prediction);

    //doesent make sense, because at the start the error is to large
    if (testStoreResampled.length < 10) {return;}
    var mean = Service.Mobis.Utils.Stat.onlineMeanError(diff);
    console.log("Total error: " + mean);
  }
});

// Clean loaded records
var records = testStore.recs;
for (var ii=0; ii<records.length; ii++) {
  var rec = records[ii];
  Service.Mobis.Loop.addNoDuplicateValues(testStoreClean, rec);
}




// ONLINE SERVICES

//http://localhost:8080/sensors/query_boss?data={"$join":{"$name":"hasMeasurement","$query":{"$from":"CounterNode","Name":"0060-11"}}}
//http://localhost:8080/sensors/query_boss?data={"$join":{"$name":"measured","$query":{"$from":"SensorType","Id":"1"}}}
//http://localhost:8080/sensors/query?data={"$from":"SensorMeasurement"}
http.onGet("query", function (req, resp) {
  jsonData = JSON.parse(req.args.data);
  console.say("" + JSON.stringify(jsonData));
  var recs = qm.search(jsonData);
	return jsonp(req, resp, recs);
});

//http://localhost:8080/sensors/getRawStore
http.onGet("getRawStore", function (req, resp) {
	var storeName = testStore.name;
	var recs = qm.search({"$from": storeName});
  return jsonp(req, resp, recs);
});

//http://localhost:8080/sensors/getCleanedStore
http.onGet("getCleanedStore", function (req, resp) {
	var storeName = testStoreClean.name;
	var recs = qm.search({"$from": storeName});
  return jsonp(req, resp, recs);
});

//http://localhost:8080/sensors/getResampledStore
http.onGet("getResampledStore", function (req, resp) {
	var storeName = testStoreResampled.name;
	var recs = qm.search({"$from": storeName});
  return jsonp(req, resp, recs);
});

//http://localhost:8080/sensors/getResampledStore
http.onGet("getFeatureVector", function (req, resp) {
  if (req.args.data) { testStore.add(JSON.parse(req.args.data)); }
  var rs = testStoreResampled.recs;
  var rec = rs[rs.length-1];
  var ftrVec = ftrSpace.ftrVec(testStoreResampled[rec.$id]);
  var strFtr = Service.Mobis.Utils.Io.printStr(ftrVec);
  return jsonp(req, resp, strFtr);
});

//http://localhost:8080/sensors/getResampledStore
http.onGet("getModel", function (req, resp) {
  if (req.args.data) { testStore.add(JSON.parse(req.args.data)); }
  var model = ridgeRegression.getModel();
  var modelString = Service.Mobis.Utils.Io.printStr(model);
  return jsonp(req, resp, modelString);
});

//localhost:8080/sensors/addCounterMeasurement?data={"DateTime":"2013-07-02T01:15:00","NumOfCars":60.0,"Gap":92.0,"Occupancy":6.0,"Speed":75.0,"TrafficStatus":1,"measuredBy":{"Name":"0016-21"}}
http.onGet("addCounterMeasurement", function (req, resp) {
  testStore.add(JSON.parse(req.args.data));
  console.say("OK addCounterMeasurement");
  return jsonp(req, resp, "OK");
});