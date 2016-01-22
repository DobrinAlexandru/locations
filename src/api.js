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
  locations: function(request) {
    return locationsUtils.handleLocationsRequest(request.payload);
  },
  apiLocations: function(request) {
    return bumpsUtils.processLocationsAndCreateOrUpdateBumps(request.payload);
  },
  userLocations: function(request) {
    return locationsUtils.getLocationsForUser(request.query);
  },
  // TODO FIX GET
  apiUserLocations: function(request) {
    return requestLib({
      url: utils.C.LOCATIONS_IP + '/userLocations',
      method: 'GET',
      qs: request.query
    });
  },

  apiLoadNewsFeed: function(request) {
    return bumpsUtils.loadNewsFeed(request.payload);
  },
  apiUpdateUser: function(request) {
    return usersUtils.updateUser(request.payload);
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
  }
};

module.exports = API;