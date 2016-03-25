var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var requestLib = Promise.promisify(require("request"));
var utils = require('./utils');

var Notifications = {
  sendNewBumpsNotification: function(newBumps, usersById) {
    var bumpsByUser = _.groupBy(newBumps, function(bump) {
      return bump._source.user1.userId;
    });
    var notifications = [];
    _.each(bumpsByUser, function(userBumps, toUserId) {
      // Agregate data for notification
      var toUser = usersById[toUserId];
      var newUsersPayload = [];
      _.each(userBumps, function(bump) {
        var user = usersById[bump._source.user2.userId];
        // Filter out users that don't match age & gender filters
        if (utils.allFiltersMatch(toUser, user) && Math.random() <= 0.33) {
          newUsersPayload.push({
            fbId: user._source.fbid,
            userId: user._id,
            userName: user._source.firstName
          });
        }
      });
      // If we have data for notification
      if (newUsersPayload.length > 0) {
        var notification = {
          toIds: [toUserId],
          data: {
            alert: {
              "loc-args": [this.listOfNames(newUsersPayload)],
              "loc-key": "INTERSECTED_WITH_NOTIFICATION"
            },
            sound: "default",
            type: "updateFeed",
            newUsers: newUsersPayload
          }
        };
        // Push notification to list
        notifications.push(notification);
      }
    }.bind(this));
    return this.sendNotificationsToParse(notifications);
  },

  sendAddFriendNotification: function(fromUser, toUserId, type) {
    var notifications = [];
    var notification = {
      toIds: [toUserId],
      data: {
        alert: {
          "loc-args": [fromUser._source.firstName],
          "loc-key": type == "add_friend" ? "ADD_FRIEND_NOTIFICATION" : "ACCEPT_FRIEND_NOTIFICATION"
        },
        sound: "default",
        type:         type,
        fromUserId:   fromUser._id,
        user: {
          fbId: fromUser._source.fbid,
          userId: fromUser._id,
          userName: fromUser._source.firstName
        }
      }
    };
    // Push notification to list
    notifications.push(notification);
    return this.sendNotificationsToParse(notifications);
  },
  sendMsgNotification: function(fromUser, toUserId, msg) {
    var notifications = [];
    var notification = {
      toIds: [toUserId],
      data: {
        alert: fromUser._source.firstName + ": " + msg,
        sound: "default",
        msg:        msg,
        from_name:  fromUser._source.firstName,
        from_id:    fromUser._id,
        fb_id:      fromUser._source.fbid,
      }
    };
    // Push notification to list
    notifications.push(notification);
    return this.sendNotificationsToParse(notifications);
  },
  sendUpdateFeedNotification: function(toUserId) {
    var notifications = [];
    var notification = {
      toIds: [toUserId],
      data: {
        sound: "default",
        type: "updateFeed",
      }
    };
    // Push notification to list
    notifications.push(notification);
    return this.sendNotificationsToParse(notifications);
  },

  listOfNames: function(users) {
    var names = _.pluck(users, "userName");
    if (names.length <= 1) {
      return names.join();
    } else {
      return names.slice(0, -1).join(", ") + " si " + _.last(names);
    }
  },
  sendNotificationsToParse: function(notifications) {
    if (notifications && notifications.length > 0) {
      // https://www.parse.com/docs/rest/guide
      return requestLib({
        url: 'https://api.parse.com/1/functions/sendNotifications',
        method: 'POST',
        headers: {
          "X-Parse-Application-Id": "XxAKpRlTJIny9YsHIb1bdoCuWJhm4InfOgI2GvWB",
          "X-Parse-REST-API-Key": "pveAqS6L5HGGpytegzUKPLLeyO9xxgsfyF0EdRh5",
          "Content-Type": "application/json"
        },
        json: true,
        body: {notifications: notifications}
      });
    } else {
      return Promise.resolve({});
    }
  },

};

module.exports = Notifications;