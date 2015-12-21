var Hapi = require('hapi');
var joi = require('joi');

var https = require('https');
var http = require('http');
var fs = require('fs');

var locationsUtils = require('./locations');

var options = {
  // key: fs.readFileSync('./intersect-key.pem'),
  // cert: fs.readFileSync('./intersect-cert.pem')
};

var server = new Hapi.Server();
server.connection({
  port: 8001,
  // port: 443,
  // listener: https.createServer(options, function (req, res) {}),
  // tls: true
});

server.route({
  method: 'POST',
  path: '/locations',
  config: {
    handler: function (request, reply) {
      locationsUtils.handleLocationsRequest(request, reply);
    }
  }
});

server.route({
    method: 'GET',
    path: '/userLocations',
    config: {
      handler: function (request, reply) {
        console.log("xx: " + JSON.stringify(request.query));
        locationsUtils.getLocationsForUser(request, reply);
      },
      cors: true
    }
});

server.register({
    register: require('good'),
    options: options
}, function (err) {
    if (err) {
        console.error(err);
    }
    else {
        server.start(function () {
            console.info('Server started at ' + server.info.uri);
        });
    }
});
