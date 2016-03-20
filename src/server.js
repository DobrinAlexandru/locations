var Hapi = require('hapi');

var apiUtils = require('./api');

// var heapdump = require('heapdump');
var _ = require('underscore');
var Promise = require("bluebird");

var options = {
};

var server = new Hapi.Server();
server.connection({
  port: process.env.PORT,
});

var postApis = {
  // "/locations":         "locations",
  // "/api/locations":     "apiLocations",
  "/api/loadNewsFeed":  "apiLoadNewsFeed",
  "/api/markBumpAsSeen":"apiMarkBumpAsSeen",

  "/api/updateUser":    "apiUpdateUser",
  "/api/updateUsers":   "apiUpdateUsers",
  "/api/loadUsers":     "apiLoadUsers",

  "/api/loadConversations":   "apiLoadConversations",
  "/api/loadMessages":        "apiLoadMessages",
  "/api/sendMessage":         "apiSendMessage",
  "/api/markConvAsRead":      "apiMarkConvAsRead",
  "/api/deleteConv":          "apiDeleteConv",

  "/api/loadInbox":           "apiLoadInbox",
  "/api/addFriend":           "apiAddFriend",
  "/api/acceptFriend":        "apiAcceptFriend",
  "/api/hideIntersection":    "apiHideIntersection",


  // "/api/2/locations":         "api2Locations",

  "/api/2/loadNewsFeed":      "api2LoadNewsFeed",
  "/api/2/loadConversations": "api2LoadConversations",
  "/api/2/loadInbox":         "api2LoadInbox",
  "/api/2/loadUsers":         "api2LoadUsers",
  "/api/2/loadMessages":      "api2LoadMessages",

  "/api/2/sendMessage":       "api2SendMessage",
  "/api/2/deleteConv":        "api2DeleteConv",

  "/api/2/markConvAsRead":    "api2MarkConvAsRead",
  "/api/2/markBumpAsSeen":    "api2MarkBumpAsSeen",

  "/api/2/addFriend":         "api2AddFriend",
  "/api/2/acceptFriend":      "api2AcceptFriend",
  "/api/2/hideIntersection":  "api2HideIntersection",


  "/api/test":          "apiTest"
};

var getApis = {
  // "/userLocations":             "userLocations",
  "/api/userLocations":         "apiUserLocations",
  
  // "/latestLocationsByUser":     "latestLocationsByUser",
  "/api/latestLocationsByUser": "apiLatestLocationsByUser"
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
          apiUtils[val](request).then(function(result) {
            // console.log("result: " + JSON.stringify(result));
            console.log(">>End ok" + key + " time " + (Date.now() - timerStart));
            reply(result);
          })
          .error(function(e) {
            console.error("result: " + JSON.stringify(e));
            console.log(">>End error" + key + " time " + (Date.now() - timerStart));
            if (val.startsWith("api2Load")) {
              reply([]);
            } else {
              reply(e);
            }
          })
          .catch(function(e) {
            console.error("result: " + JSON.stringify(e));
            console.log(">>End error" + key + " time " + (Date.now() - timerStart));
            if (val.startsWith("api2Load")) {
              reply([]);
            } else {
              reply(e);
            }
          });
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
            apiUtils.forceUpdateUsers().then(function() {
              console.log("Users have been updated");
            });
        });
    }
});
