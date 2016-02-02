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
  // "/api/locations":     "apiLocations",
  // "/api/loadNewsFeed":  "apiLoadNewsFeed",
  // "/api/markBumpAsSeen":"apiMarkBumpAsSeen",

  // "/api/updateUser":    "apiUpdateUser",
  // "/api/updateUsers":   "apiUpdateUsers",
  // "/api/loadUsers":     "apiLoadUsers",

  // "/api/loadConversations":   "apiLoadConversations",
  // "/api/loadMessages":        "apiLoadMessages",
  // "/api/sendMessage":         "apiSendMessage",
  // "/api/markConvAsRead":      "apiMarkConvAsRead",
  // "/api/deleteConv":          "apiDeleteConv",

  // "/api/loadInbox":           "apiLoadInbox",
  // "/api/addFriend":           "apiAddFriend",
  // "/api/acceptFriend":        "apiAcceptFriend",
  // "/api/hideIntersection":    "apiHideIntersection",

  // "/api/test":          "apiTest"
};

var getApis = {
  "/userLocations":             "userLocations",
  // "/api/userLocations":         "apiUserLocations",
  
  "/latestLocationsByUser":     "latestLocationsByUser",
  // "/api/latestLocationsByUser": "apiLatestLocationsByUser"
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
          console.log("payload: " + JSON.stringify(request.payload));
          // Call api method
          apiUtils[val](request).then(function(result) {
            console.log("result: " + JSON.stringify(result));
            console.log(">>End ok" + key + " time " + (Date.now() - timerStart));
            reply(result);
          }).error(function(e) {
            console.error("result: " + JSON.stringify(e));
            console.log(">>End error" + key + " time " + (Date.now() - timerStart));
            reply(e);
          });
          // .catch(function(e) {
          //   console.error("result: " + JSON.stringify(e));
          //   console.log(">>End error" + key + " time " + (Date.now() - timerStart));
          //   reply(e);
          // });
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
            // apiUtils.forceUpdateUsers().then(function() {
            //   console.log("Users have been updated");
            // });
        });
    }
});
