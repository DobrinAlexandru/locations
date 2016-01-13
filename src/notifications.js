var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var requestLib = Promise.promisify(require("request"));

var Notifications = {
  sendNewBumpsNotification: function(newBumps, usersById) {
    var bumpsByUser = _.groupBy(newBumps, function(bump) {
      return bump._source.user1.userId;
    });
    var notifications = [];
    _.each(bumpsByUser, function(userBumps, toUserId) {
      // Agregate data for notification
      var toUser = usersById[toUserId];
      var newUsersPayload = _.map(userBumps, function(bump) {
        var user = usersById[bump._source.user2.userId];
        return {
          fbId: user.fbid,
          userId: user.userId,
          userName: user.shortName
        }
      });
      // If we have data for notification
      if (newUsersPayload.length > 0) {
        var notification = {
          toIds: [toUserId],
          data: {
            alert: {
              "loc-args": [this.listOfNames(newUsers)],
              "loc-key": "INTERSECTED_WITH_NOTIFICATION"
            },
            sound: "default",
            type: "updateFeed",
            newUsers: newUsersPayload
          }
        }
        // Push notification to list
        notifications.push(notification);
      }
    });
    if (notifications.length > 0) {
      // Send notification request to Parse
      return this.sendNotificationsToParse(notifications);
    } else {
      return Promise.resolve({});
    }
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
    // https://www.parse.com/docs/rest/guide
    return requestLib({
      url: 'https://api.parse.com/1/functions/sendNotifications',
      method: 'POST',
      headers: {
        "X-Parse-Application-Id": "0aEW2TDQeZPog4Yq8hsxxE50gOJVGYmWNGLuo6px",
        "X-Parse-REST-API-Key": "TbVX6Aa32BxAyyWlgxDpo1vOIQzC4yOs0yv7rdAn",
        "Content-Type": "application/json"
      },
      json: true,
      body: {notifications: notifications}
    });
  },

};

module.exports = Notifications;