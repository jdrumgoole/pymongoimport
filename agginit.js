db = connect("localhost/vosa");

ageinusecs = { "$subtract" : [ "$TestDate", "$FirstUseDate" ] }
ageinyears = { "$divide" :[ ageinusecs , (1000*3600*24*365) ] }
floorage = { "$floor" : ageinyears }
ispass =  { "$cond" : [{"$eq": ["$TestResult","P"]},1,0]}
project = { "$project" : { "Make":1, "VehicleID" : 1, "TestResult":1, "TestDate":1,"TestMileage":1,"FirstUseDate":1,"Age":floorage,"pass":ispass }}

removeNulls = { "$match" : { "FirstUseDate" : { "$ne" : "NULL" }}};
goodDates = { "$out" : "goodDates" };

carsonly = { "$match" : { "TestClassID" : 4 }} ;

carsWithPasses = { "$match" : { "TestClassID" : { "$eq" : "4" }, "TestResult" : "P" }} ;

knownage = { "$match" : {  "FirstUseDate" : { "$ne" : "NULL" }}};

onlyPasses = { "$match" : { "pass" : 1 }};
group = { "$group" : { "_id" : { "make": "$Make", "age" : "$Age" }, "count" : {"$sum":1} , "miles": {"$avg":"$TestMileage"},"passes":{"$sum":"$pass"}}};

out = { "$out" : "cars_summary" } ;

//main

avgMilesPerMake = { "$group" : { "_id" : "$Make", "avgMiles" : { "$avg" : "$TestMileage"}}} ;

//Summary

countPasses = { "$group" : { "_id" : "$_id.make", "passCount" : {"$sum" : "$passes" }}} ; 

countVehicles = { "$group" : { "_id" : "$VehicleID", "count" : { "$sum" : 1 }}} ;
countMakes = { "$group" : { "_id" : "$Make"} , "total" : {"$sum" : 1 }} ;

countMiles = { "$group" : { "_id" : "$Make" , "totalMileage" : {"$sum" : "$TestMileage" }}} ;


