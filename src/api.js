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

var API = {
  // GET API
  userLocations: function(request) {
    return locationsUtils.getLocationsForUser(request.query);
  },
  // GET API
  apiUserLocations: function(request) {
    return requestLib({
      url: utils.C.LOCATIONS_IP + '/userLocations',
      method: 'GET',
      json: true,
      qs: request.query
    }).get(1);
  },
  // GET API
  latestLocationsByUser: function(request) {
    return locationsUtils.getLatestLocationsByUser(request.query);
  },
  // GET API
  apiLatestLocationsByUser: function(request) {
    return requestLib({
      url: utils.C.LOCATIONS_IP + '/latestLocationsByUser',
      method: 'GET',
      json: true,
      qs: request.query
    }).get(1);
  },
  locations: function(request) {
    return locationsUtils.handleLocationsRequest(request.payload);
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
        'X-Parse-Application-Id': '0aEW2TDQeZPog4Yq8hsxxE50gOJVGYmWNGLuo6px',
        'X-Parse-REST-API-Key': 'TbVX6Aa32BxAyyWlgxDpo1vOIQzC4yOs0yv7rdAn'
      },
      json: true,
      body: {}
    });
  }
};

module.exports = API;