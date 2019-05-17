var express = require('express');
const https = require('https');
const http = require('http');
const request = require('request');
const axios = require("axios");
const fs = require('fs');
const random = require('random')

var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  console.log("tole")
  // res.render('index', { title: 'Express' });

  fs.readFile('./url_list.json', (err, data) => {
    if (err) throw err;
    let url_list = JSON.parse(data);
    var url_count = Object.keys(url_list).length;
    
    var selected_url = "url_" + random.int(min = 1, max = 5); // uniform integer in [ min, max ];
    console.log(selected_url);
    console.log(url_list[selected_url]);

    res.redirect("http://" + url_list[selected_url]);
  });
  
});

function random_item(items) {

  return items[Math.floor(Math.random() * items.length)];

}

/* GET home page. */
router.get('/tole', function (req, res, next) {  
  res.redirect("http://www.google.com");
});

module.exports = router;
