var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');

var requestLib = Promise.promisify(require("request"));

var locationsUtils = require('./locations');
var bumpsUtils = require('./bumps');
var notificationsUtils = require('./notifications');
var usersUtils = require('./users');
var conversationsUtils = require('./conversations');
var addFriendUtils = require('./addFriend');
var utils = require('./utils');
var classes = require('./classes');

var API = {
  // GET API
  // userLocations: function(request) {
  //   return locationsUtils.getLocationsForUser(request.query);
  // },
  // GET API
  apiUserLocations: function(request) {
    // return requestLib({
    //   url: utils.C.LOCATIONS_IP + '/userLocations',
    //   method: 'GET',
    //   json: true,
    //   qs: request.query
    // }).get(1);
    return locationsUtils.getLocationsForUser(request.query);
  },
  // GET API
  // latestLocationsByUser: function(request) {
  //   return locationsUtils.getLatestLocationsByUser(request.query);
  // },
  // GET API
  apiLatestLocationsByUser: function(request) {
    // return requestLib({
    //   url: utils.C.LOCATIONS_IP + '/latestLocationsByUser',
    //   method: 'GET',
    //   json: true,
    //   qs: request.query
    // }).get(1);
    return locationsUtils.getLatestLocationsByUser(request.query);
  },
  // locations: function(request) {
  //   return locationsUtils.handleLocationsRequest(request.payload);
  // },api2MacObjects
  apiLocationsMacObjects: function(request) {
    console.log("macobjects api");
    return bumpsUtils.processMacAddressAndCreateOrUpdateBumps(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },
  apiLocations: function(request) {
    return bumpsUtils.processLocationsAndCreateOrUpdateBumps(request.payload);
  },
  apiLoadNewsFeed: function(request) {
    return bumpsUtils.loadNewsFeed(request.payload);
  },
  apiMarkBumpAsSeen: function(request) {
    return bumpsUtils.markBumpAsSeen(request.payload);
  },

  apiUpdateUser: function(request) {
    return usersUtils.updateUser(request.payload);
  },
  apiUpdateUsers: function(request) {
    return usersUtils.updateUsers(request.payload);
  },
  apiLoadUsers: function(request) {
    return usersUtils.loadUsers(request.payload);
  },

  apiLoadConversations: function(request) {
    return conversationsUtils.loadConversations(request.payload);
  },
  apiLoadMessages: function(request) {
    return conversationsUtils.loadMessages(request.payload);
  },
  apiSendMessage: function(request) {
    return conversationsUtils.sendMessage(request.payload);
  },
  apiMarkConvAsRead: function(request) {
    return conversationsUtils.markConversationAsRead(request.payload);
  },
  apiDeleteConv: function(request) {
    return conversationsUtils.deleteConversation(request.payload);
  },

  apiLoadInbox: function(request) {
    return addFriendUtils.loadInbox(request.payload);
  },
  apiAddFriend: function(request) {
    return addFriendUtils.addFriend(request.payload);
  },
  apiAcceptFriend: function(request) {
    return addFriendUtils.acceptFriend(request.payload);
  },
  apiHideIntersection: function(request) {
    return addFriendUtils.hideIntersection(request.payload);
  },

  api2Locations: function(request) {
    return bumpsUtils.processLocationsAndCreateOrUpdateBumps(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },

  api2LocationTimeMachine : function(request){
    console.log("enter timemachine" + JSON.stringify(request.payload));
    var locations =  locationsUtils.mapLocationsToDBModel(request.payload.locations, request.payload.userId);
    return locationsUtils.getUserForTimeMachine(locations, request.payload.userId, 0, request.payload.gender, 
      request.payload.interesteInMin, request.payload.interestedInMax, request.payload.age, request.payload.tryFakeUsers, request.payload.genderInt).then(function(results) {
          return Promise.resolve(results);
    });
  },

  api2LocationsMacObjects: function(request) {
    console.log("macobjects api");
    return bumpsUtils.processMacAddressAndCreateOrUpdateBumps(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },

  api2LoadNewsFeed: function(request) {
    return bumpsUtils.loadNewsFeed(request.payload).then(function(results) {
      results = _.map(results, function(pair) {
        return new classes.WBump(pair.bump, pair.user, pair.conversation);
      });
      return Promise.resolve(results);
    });
  },
  api2LoadConversations: function(request) {
    return conversationsUtils.loadConversations(request.payload).then(function(results) {
      results = _.map(results, function(pair) {
        return new classes.WConversation(pair.conversation, pair.user, pair.bump);
      });
      return Promise.resolve(results);
    });
  },
  api2LoadInbox: function(request) {
    return addFriendUtils.loadInbox(request.payload).then(function(results) {
      results = _.map(results, function(pair) {
        return new classes.WInbox(pair.bump, pair.user1, pair.user2);
      });
      return Promise.resolve(results);
    });
  },
  api2LoadUsers: function(request) {
    return usersUtils.loadUsers(request.payload).then(function(results) {
      results = _.map(results, function(user) {
        return new classes.WUser(user);
      });
      return Promise.resolve(results);
    });
  },
  api2LoadMessages: function(request) {
    return conversationsUtils.loadMessages(request.payload).then(function(results) {
      results = _.map(results, function(msg) {
        return new classes.WMessage(msg);
      });
      return Promise.resolve(results);
    });
  },

  api2SendMessage: function(request) {
    return conversationsUtils.sendMessage(request.payload).then(function(results) {
      results = new classes.WMessage(results);
      return Promise.resolve(results);
    });
  },
  api2DeleteConv: function(request) {
    return conversationsUtils.deleteConversation(request.payload).then(function(conv) {
      results = new classes.WConversation(conv);
      return Promise.resolve(results);
    });
  },

  api2MarkConvAsRead: function(request) {
    return conversationsUtils.markConversationAsRead(request.payload).then(function(results) {
      return Promise.resolve({});
    });
  },
  api2MarkBumpAsSeen: function(request) {
    return bumpsUtils.markBumpAsSeen(request.payload).then(function(results) {
      return Promise.resolve({status: {status: "success"}});
    });
  },

  api2AddFriend: function(request) {
    return addFriendUtils.addFriend(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },
  api2AcceptFriend: function(request) {
    return addFriendUtils.acceptFriend(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },
  api2HideIntersection: function(request) {
    return addFriendUtils.hideIntersection(request.payload).then(function(results) {
      return Promise.resolve({status: "success"});
    });
  },


  apiTest: function(request) {
    return notificationsUtils.sendNotificationsToParse(request.payload.notifications);
  },

  // This calls parse
  // Parse fetches all dirty users & sends them to server to update
  // Then parse updates the dirty keys on those users
  forceUpdateUsers: function() {
    return requestLib({
      url: 'https://api.parse.com/1/functions/updateUsers',
      method: 'POST',
      headers: {
        // TODO Change api keys
        'X-Parse-Application-Id': 'XxAKpRlTJIny9YsHIb1bdoCuWJhm4InfOgI2GvWB',
        'X-Parse-REST-API-Key': 'pveAqS6L5HGGpytegzUKPLLeyO9xxgsfyF0EdRh5'
      },
      json: true,
      body: {}
    });
  }
};

module.exports = API;