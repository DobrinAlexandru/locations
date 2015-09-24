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
  port: 3000,
  listener: https.createServer(options, function (req, res) {}),
  tls: true
});

server.route({
    method: 'GET',
    path: '/locations',
    handler: function (request, reply) {
      var db = request.server.plugins['hapi-mongodb'].db;
      db.collection('locations').find().toArray(function (err, doc){
        reply(doc);
      });
    }
});

server.route({
  method: 'POST',
  path: '/locations',
  config: {
    handler: function (request, reply) {
      var newLoc = {
        id: request.payload.id
      };
      var db = request.server.plugins['hapi-mongodb'].db;
      db.collection('locations').insert(newLoc, { w: 1 }, function (err, doc){
        if (err){
          return reply(Hapi.error.internal('Internal MongoDB error', err));
        } else {
          reply(doc);
        }
      });
    },

    validate: {
      payload: {
        id: joi.string().required(),
        // note: joi.string().required()
      }
    }
  }
});

server.route({
    method: 'GET',
    path: '/',
    handler: function (request, reply) {
        reply('loc API v0.1\n');
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
