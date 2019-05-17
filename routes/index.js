var express = require('express');
const https = require('https');
const http = require('http');
const request = require('request');
const axios = require("axios");
var router = express.Router();

var db = require('odbc')()
  , cn = "DRIVER={Cloudera ODBC Driver for Impala};host=140.116.234.166;port=1962";
  // , cn = "DRIVER={Cloudera ODBC Driver for Impala};host=140.116.86.246;port=1000";
db.openSync(cn);

function intervalInsertPredict() {
  // console.log('Cant stop me now!');

  const url = "http://140.116.234.166:23005/predictor/";
  const getData = async url => {
    try {
      const response = await axios.get(url);
      const data = response.data;
      console.log((data));

      var target = data.target;
      var predicted_result = data.predicted_result;
      var sql = "select index, target, predicted_result from predict order by index desc limit 1;";
      console.log(sql);
      
      db.query(sql, function (err, select_data) {
        if (err) {
          console.error(err);
        } else {          
          var db_index = parseInt(select_data[0].index);
          var db_target = (select_data[0].target);
          var db_predicted_result = (select_data[0].predicted_result);
          if ((db_target != target || db_predicted_result != predicted_result) && (target != null && predicted_result != null) ) {
            var sql = "INSERT INTO predict VALUES (" + (db_index+1) + ", '" + target + "', '" + predicted_result + "', current_timestamp());";
            console.log(sql);
            db.query(sql, function (err, insert_data) {
              if (err) {
                console.error(err);
              } else {
                console.log(insert_data);
              }
            });
          } else { console.log("Value exists!") }
        }
      });

      // console.log(url);
    } catch (error) {
      console.log(error);
    }
  };

  const c_t_url = "http://140.116.234.166:23005/data/current_and_temperature/";
  const getData_c_t = async url => {
    try {
      const response = await axios.get(url);
      const data = response.data;
      console.log((data));

      var target = data.target;
      var current = data.current;
      var temperature = data.temperature;
      // var sql = "SELECT count(target) as count_target FROM predict WHERE target = '" + target + "'";
      var sql = "select index, target, elec_current, temperature from current_and_temp_live order by index desc limit 1;";
      console.log(sql);

      db.query(sql, function (err, select_data) {
        if (err) {
          console.error(err);
        } else {
          var db_index = parseInt(select_data[0].index);
          var db_elec_current = parseFloat(select_data[0].elec_current);
          var db_temperature = parseFloat(select_data[0].temperature);
          var db_target = (select_data[0].target);
          // if (count_target < 1) {
          if ((db_elec_current != current || db_temperature != temperature) && (temperature != null && current != null) ) {
            // var sql = "INSERT INTO predict VALUES ('" + target + "', '" + predicted_result + "', current_timestamp());";
            var sql = "INSERT INTO current_and_temp_live VALUES (" + (db_index+1) + ", '" + target + "', " + current + ", " + temperature + ");";
            console.log(sql);
            db.query(sql, function (err, insert_data) {
              if (err) {
                console.error(err);
              } else {
                console.log(insert_data);
              }
            });
          } else { console.log("Value exists!") }
        }
      });

      // console.log(url);
    } catch (error) {
      console.log(error);
    }
  };

  getData(url);
  getData_c_t(c_t_url);
}

setInterval(intervalInsertPredict, 10000);

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/vibration', function(req, res, next) {
  res.render('vibration', { title: 'Vibration' });
});

router.get('/current', function(req, res, next) {
  res.render('current', { title: 'Current and Temperature' });
});

router.get('/predictionHistory', function(req, res, next) {
  res.render('predictionHistory', { title: 'Prediction History' });
});

router.get('/dq/:yourQuery', function (req, res) {
  console.log('GET one requirement: DirectQuery');
  var sql = req.params.yourQuery;
  console.log(sql);

  db.query(sql, function (err, data) {
    if (err) {
      console.error(err);
      res.send(err);
    } else {
      console.log(data);
      res.json(data);
    }
  });
});


router.get('/insert', function (req, res) {
  // console.log('GET one requirement: DirectQuery');
  var target = req.query.target;
  var predicted_result = req.query.predicted_result;
  var sql = "INSERT INTO predict VALUES ('" + target + "', '" + predicted_result +"', current_timestamp());";
  console.log(sql);

  db.query(sql, function (err, data) {
    if (err) {
      console.error(err);
      res.send(err);
    } else {
      console.log(data);
      res.json(data);
    }
  });
});

router.get('/latest', function (req, res) {
  // console.log('GET one requirement: DirectQuery');  
  var sql = "SELECT * FROM predict ORDER BY `timestamp` DESC LIMIT 1;";
  console.log(sql);

  db.query(sql, function (err, data) {
    if (err) {
      console.error(err);
      res.send(err);
    } else {
      console.log(data);
      res.json(data);
    }
  });
});

router.get('/latest_current_temp', function (req, res) {
  // console.log('GET one requirement: DirectQuery');  
  var sql = "SELECT target, elec_current, temperature FROM current_and_temp_live ORDER BY index DESC LIMIT 1;";
  console.log(sql);

  db.query(sql, function (err, data) {
    if (err) {
      console.error(err);
      res.send(err);
    } else {
      console.log(data);
      res.json(data);
    }
  });
});

/* GET home page. */
router.get('/tole', function (req, res, next) {
  res.render('tole', { title: 'Tole' });
});

module.exports = router;
