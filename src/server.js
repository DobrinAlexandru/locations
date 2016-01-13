var Hapi = require('hapi');

var apiUtils = require('./api');

var _ = require('underscore');
var Promise = require("bluebird");

var options = {
};

var server = new Hapi.Server();
server.connection({
  port: 8001,
});

var postApis = {
  "/locations":         "locations",
  "/api/locations":     "apiLocations",
  "/api/updateBumps":   "apiUpdateBumps",
  "/api/loadNewsFeed":  "apiLoadNewsFeed",
  "/api/updateUser":    "apiUpdateUser",
  "/api/test":          "apiTest"
};

var getApis = {
  "/userLocations":     "userLocations",
  "/api/userLocations": "apiUserLocations"
};

function createRoutes(routes, method, cors) {
  _.each(routes, function(val, key) {
    server.route({
      method: method,
      path: key,
      config: {
        handler: function(request, reply) {
          // Track time
          var timerStart = Date.now();
          console.log("\n<<Start " + key);
          // console.log("payload: " + JSON.stringify(request.payload));
          // Call api method
          apiUtils[val](request, reply);
          console.log(">>End " + key + " time " + (Date.now() - timerStart));
        },
        cors: cors
      }
    });
  });
}

createRoutes(postApis, "POST");
createRoutes(getApis, "GET", true);

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
