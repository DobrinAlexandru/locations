var Hapi = require('hapi');
var joi = require('joi');

var https = require('https');
var http = require('http');
var fs = require('fs');

var options = {
  key: fs.readFileSync('./intersect-key.pem'),
  cert: fs.readFileSync('./intersect-cert.pem')
};

var server = new Hapi.Server();
server.connection({
  port: 80,
  // port: 443,
  // listener: https.createServer(options, function (req, res) {}),
  // tls: true
});

server.route({
    method: 'GET',
    path: '/locations',
    handler: function (request, reply) {
      var db = request.server.plugins['hapi-mongodb'].db;
      db.collection('locations').find().toArray(function (err, doc){
        reply({ "success" : doc });
      });
    }
});

server.route({
  method: 'POST',
  path: '/locations',
  handler: function (request, reply) {
    console.log(request.payload);
    var locations = request.payload.data;
    var db = request.server.plugins['hapi-mongodb'].db;
    db.collection('locations').insert(locations[0], { w: 1 }, function (err, doc){
      if (err){
        return reply({"error": Hapi.error.internal('Internal MongoDB error', err)});
      } else {
        // reply({"success": doc});
        reply({
          "processedLocations":[
            {
              "location":{"time":1420730570552,"longitude":"0.69","latitude":"-0.898989"},
              "nearbyLocations": ["Jet30Vt2Ed","BjfUIB3dXO"]
            }
          ],
          "success": doc
        })
      }
    });
  }
});

server.route({
    method: 'GET',
    path: '/',
    handler: function (request, reply) {
        reply({success: "loc v0.3 !!!!!"});
    }
});


var dbOpts = {
  "url"       : "mongodb://localhost:27017/locations",
  "options"   : {
    "db"    : {
      "native_parser" : false
    }
  }
};

server.register([
  {
    register: require('good'),
    pluginOptions: {
        reporters: [{
            reporter: require('good-console'),
            events: {
                response: '*',
                log: '*'
            }
        }]
    }
  },
  {
      register: require('hapi-mongodb'),
      pluginOptions: dbOpts
  }
], function (err) {
    if (err) {
        console.error(err);
        throw err; // something bad happened loading the plugin
    }

    server.start(function () {
        server.log('info', 'Server running at: ' + server.info.uri);
    });
});
