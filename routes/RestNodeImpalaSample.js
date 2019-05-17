var express = require('express');
var app = express();
var db = require('odbc')()
	, cn = "DRIVER={Cloudera ODBC Driver for Impala};host=140.116.234.166;port=1962";
  // , cn = "DRIVER={Cloudera ODBC Driver for Impala};host=192.168.0.91;port=21050";

//open impala database connection
db.openSync(cn);

var nodeServerPort = 8082;

//use: http://nodeServerIP:nodeServerPort/dq/YourQuery
//e.g. http://127.0.0.1:8082/dq/SELECT * FROM tmp WHERE id = 1
app.get('/dq/:yourQuery', function (req, res){
	console.log('GET one requirement: DirectQuery');
	var sql = req.params.yourQuery;
	console.log(sql);
	
	db.query(sql, function (err, data){
		if (err){
			console.error(err);
			res.send(err);
		} else {
			console.log(data);
			res.json(data);
		}
	});
});

var server = app.listen(nodeServerPort);