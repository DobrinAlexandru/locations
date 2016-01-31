var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var requestLib = Promise.promisify(require("request"));

var notificationsUtils = require('./notifications');
var conversationsUtils = require('./conversations');
var utils = require('./utils');

var AddFriend = {
  addFriend: function(payload) {
    var fromUserId = payload.fromUserId;
    var toUserId = payload.toUserId;

    return Promise.all([
      dbh.fetchObject(fromUserId + toUserId, "bumps", "bump"),
      dbh.fetchObject(toUserId + fromUserId, "bumps", "bump")
    ]).bind(this).spread(function(bump1, bump2) {
      console.log("1 " + JSON.stringify(bump1));
       var switchPromise;
       switch(bump1._source.friendStatus) {
          case 3: {
            // Already friends
            switchPromise = Promise.reject("Already friends");
            break;
          }
          case 1: {
            // Already sent the friend request.
            switchPromise = Promise.reject("Friend request already sent");
            break;
          }
          case 2: {
            // I already have a friend request pending. In this case, I must accept the friend request.
            switchPromise = this.acceptFriendRequest(fromUserId, toUserId, bump1, bump2);
            break;
          }
          case 4: {
            // Someone declined the intersection
            switchPromise = Promise.reject("Friend request failed: declined");
            break;
          }
          default: {
            // Create friend request
            switchPromise = this.createFriendRequest(fromUserId, toUserId, bump1, bump2);
            break;
          }
        }
        return switchPromise;
    });
  },
  acceptFriend: function(payload) {
    var fromUserId = payload.fromUserId;
    var toUserId = payload.toUserId;

    return Promise.all([
      dbh.fetchObject(fromUserId + toUserId, "bumps", "bump"),
      dbh.fetchObject(toUserId + fromUserId, "bumps", "bump"),
    ]).bind(this).spread(function(bump1, bump2) {
      var switchPromise;
      switch(bump1._source.friendStatus) {
          case 3: {
            // Already friends
            switchPromise = Promise.reject("Already friends");
            break;
          }
          case 1: {
            // Already sent the friend request. Can't accept your own friend request.
            switchPromise = Promise.reject("Friend request already sent. Can't accept your own friend request");
            break;
          }
          case 2: {
            // I have a friend request pending. In this case, I must accept the friend request.
            switchPromise = this.acceptFriendRequest(fromUserId, toUserId, bump1, bump2);
            break;
          }
          case 4: {
            // Someone declined the intersection.
            switchPromise = Promise.reject("Accept friend request failed: declined");
            break;
          }
          default: {
            // Unknown state
            switchPromise = Promise.reject("There is no friend request to accept");
            break;
          }
      }
      return switchPromise;
   });
  },
  hideIntersection: function(payload) {
    var fromUserId = payload.fromUserId;
    var toUserId = payload.toUserId;

    // load bumps
    return Promise.all([
      dbh.fetchObject(fromUserId + toUserId, "bumps", "bump"),
      dbh.fetchObject(toUserId + fromUserId, "bumps", "bump"),
      dbh.fetchObject(utils.keys(fromUserId, toUserId), "conversations", "conversation")
    ]).spread(function(bump1, bump2, conversation){
      var itemsToSave = [];
      if (bump1._source) {
        bump1.doc = {
          friendStatus: 4,
          hidden: true,
          seen: true
        };
        itemsToSave.push(bump1);
      }
      if (bump2._source) {
        bump2.doc = {
          friendStatus: 4,
          hidden: true
        };
        itemsToSave.push(bump2);
      }
      if (conversation._source) {
        conversation.doc = {
          user1: {
            deleted: true
          },
          user2: {
            deleted: true
          }
        };
        itemsToSave.push(conversation);
      }
      notificationsUtils.sendUpdateFeedNotification(toUserId);
      return dbh.updateListToDB(itemsToSave);
    });
  },
  loadInbox: function(payload) {
    var userId = payload.currentUserId;
    var skip = payload.skip;
    var limit = payload.limit;
    return dbh.fetchObject(userId, "users", "user").bind(this).then(function(user) {
      return dbh.loadBumps({user: user, friendStatus: 2, sort: true, filters: true}, false, skip, limit);
    }).then(function(bumps) {
      bumps = bumps.hits.hits;
      console.log("1 " + bumps.length);
      var otherUsersIds = utils.getOtherUsersIds(userId, bumps);
      otherUsersIds.push(userId);
      if (bumps.length === 0) {
        return Promise.all([
          Promise.resolve([]),
          Promise.resolve({docs: []})
        ]);
      }
      // Load users & bumps & attach to each conversation
      return Promise.all([
        Promise.resolve(bumps),
        dbh.fetchMultiObjects(otherUsersIds, "users", "user"),
      ]);
    }).spread(function(bumps, users) {
      users = users.docs;
      return Promise.resolve(this.attachUsers(userId, bumps, users));
    });
  },
  attachUsers: function(userId, bumps, users) {
    // User hash
    var withUsersHash = _.object(_.map(users, function(user) {
      return [user._id, user];
    }));
    var user = withUsersHash[userId];
    var results = _.map(bumps, function(bump) {
      var otherId = bump._source.user2.userId;
      return {
        bump: bump,
        user1: user,
        user2: withUsersHash[otherId]
      };
    });
    // Filter out bad results
    results = _.filter(results, function(obj) {
      return !!(obj.user1 && obj.user2);
    });
    return results;
  },
  createFriendRequest: function(fromUserId, toUserId, bump1, bump2) {
    bump1.doc = {
      friendStatus: 1,
      seen: true,
    };
    bump2.doc = {
      friendStatus: 2,
    };

    return Promise.all([
      dbh.updateListToDB([bump1, bump2]),
      dbh.fetchObject(fromUserId, "users", "user").then(function(fromUser) {
        // Don't wait for notification
        notificationsUtils.sendAddFriendNotification(fromUser, toUserId, "add_friend");
        return Promise.resolve({});
      })
    ]);
  },
  // fromUserId is the id of the user that made the post request.
  acceptFriendRequest: function(fromUserId, toUserId, bump1, bump2) {
    // Mark as friends
    bump1.doc = {
      friendStatus: 3,
      seen: true,
    };
    bump2.doc = {
      friendStatus: 3,
      seen: true,
    };
    return Promise.all([
      dbh.updateListToDB([bump1, bump2]),
      dbh.fetchObject(fromUserId, "users", "user").then(function(fromUser) {
        // Don't wait for notification to send
        notificationsUtils.sendAddFriendNotification(fromUser, toUserId, "accept_friend");
        return Promise.resolve({});
      })
    ]).then(function(result) {
      // Create conversation if it doesn't exist
      return dbh.fetchObject(utils.keys(fromUserId, toUserId), "conversations", "conversation")
        .then(function(conversation) {
          if (!conversation._source) {
            // Fetch users and create conversation
            return dbh.fetchMultiObjects([fromUserId, toUserId], "users", "user").bind(this).then(function(users) {
              users = users.docs;
              fromUser = users[0];
              toUser = users[1];
              return conversationsUtils.createConversation(fromUser, toUser);
            });
          } else {
            return Promise.resolve([]);
          }
        });
    });
  }
};
module.exports = AddFriend;
