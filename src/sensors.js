var analytics = require('analytics');

//import "functions.j
var Service = {}; Service.Mobis = {}; Service.Mobis.Utils = {};
Service.Mobis.Utils.RidgeRegression = require('Service/Mobis/Utils/ridgeRegression.js');
Service.Mobis.Loop = require('Service/Mobis/Loop/preproc.js');

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
                                            { "name" : "Prediction", "type" : "float", "null" : true}]);

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

// First cleaning
var records = testStore.recs;
for (var ii=0; ii<records.length; ii++) {
  var rec = records[ii];
  Service.Mobis.Loop.addNoDuplicateValues(testStoreClean, rec);
}




// ONLINE SERVICES

//http://localhost:8080/sensors/query_boss?data={"$join":{"$name":"hasMeasurement","$query":{"$from":"CounterNode","Name":"0060-11"}}}
//http://localhost:8080/sensors/query_boss?data={"$join":{"$name":"measured","$query":{"$from":"SensorType","Id":"1"}}}
//http://localhost:8080/sensors/query_boss?data={"$from":"SensorMeasurement"}
http.onGet("query", function (req, resp) {
    jsonData = JSON.parse(req.args.data);
    console.say("" + JSON.stringify(jsonData));
    var recs = qm.search(jsonData);
	jsonp(req, resp, recs);
});

//localhost:8080/sensors/addCounterMeasurement_0016_21?data={"DateTime":"2013-07-02T01:15:00","NumOfCars":60.0,"Gap":92.0,"Occupancy":6.0,"Speed":75.0,"TrafficStatus":1,"measuredBy":{"Name":"0016-21"}}
http.onGet("addCounterMeasurement_0016_21", function (req, resp) {
    testStore.add(JSON.parse(req.args.data));
    console.say("OK addCounterMeasurement_0016_21");
    return jsonp(req, resp, "OK");
});