var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');

var requestLib = Promise.promisify(require("request"));

var locationsUtils = require('./locations');
var bumpsUtils = require('./bumps');
var notificationsUtils = require('./notifications');
var usersUtils = require('./users');

var API = {
  locations: function(request, reply) {
    locationsUtils.handleLocationsRequest(request.payload)
      .then(function(result) {
        reply(result);
      }).catch(function(e) {
        reply(e);
      });
  },
  apiLocations: function(request, reply) {
    bumpsUtils.processLocationsAndCreateOrUpdateBumps(request.payload)
      .then(function(result) {
        reply(result);
      }).catch(function(e) {
        reply(e);
      });
  },
  userLocations: function(request, reply) {
    locationsUtils.getLocationsForUser(request.query)
      .then(function(result) {
        reply(result);
      }).catch(function(e) {
        reply(e);
      });
  },
  // TODO FIX GET
  apiUserLocations: function(request, reply) {
    requestLib({
      url: 'http://78.46.230.72:8001/userLocations',
      method: 'GET',
      qs: request.query
    }).then(function(result) {
      reply(result);
    }).catch(function(e) {
      reply(e);
    });
  },

  apiLoadNewsFeed: function(request, reply) {
    
  },
  apiUpdateUser: function(request, reply) {
    usersUtils.updateUser(request.payload.userId, request.payload.updateData, request.payload.newObject)
      .then(function(result) {
        reply(result);
      }).catch(function(e) {
        reply(e);
      });
  },
  apiTest: function(request, reply) {
    console.log(request.payload);
    notificationsUtils.sendNotificationsToParse(request.payload.notifications)
      .then(function(result) {
        reply(result);
      }).catch(function(e) {
        reply(e);
      });
  }
};

module.exports = API;