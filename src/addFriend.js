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
    var inboxItemId = payload.inboxItemId;

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
            switchPromise = this.acceptFriendRequest(fromUserId, toUserId, bump1, bump2, inboxItemId);
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
   }).then(function(result) {
      return dbh.fetchObject(utils.keys(fromUserId, toUserId), "conversations", "conversation")
        .then(function(conversation) {
          if (!conversation._source) {
            return conversationsUtils.createConversation(fromUserId, toUserId);
          } else {
            return Promise.resolve([]);
          }
        });
    });
  },
  hideIntersection: function(payload) {
    var fromUserId = payload.fromUserId;
    var toUserId = payload.toUserId;

    // load bumps
    return Promise.all([
      dbh.fetchObject(fromUserId + toUserId, "bumps", "bump"),
      dbh.fetchObject(toUserId + fromUserId, "bumps", "bump"),
      dbh.fetchObject(toUserId + fromUserId, "inbox", "friendRequest"),
      dbh.fetchObject(fromUserId + toUserId, "inbox", "friendRequest"),
      dbh.fetchObject(utils.keys(fromUserId, toUserId), "conversations", "conversation")
    ]).spread(function(bump1, bump2, inboxItem1, inboxItem2, conversation){
      var itemsToSave = [];
      if (bump1) {
        bump1.doc = {
          friendStatus: 4,
          hidden: true,
          seen: true
        };
        itemsToSave.push(bump1);
      }
      if (bump2) {
        bump2.doc = {
          friendStatus: 4,
          hidden: true
        };
        itemsToSave.push(bump2);
      }
      if (inboxItem1) {
        inboxItem1.doc = {
          hidden: true
        };
        itemsToSave.push(inboxItem1);
      }
      if (inboxItem2) {
        inboxItem2.doc = {
          hidden: true
        };
        itemsToSave.push(inboxItem2);
      }
      if (conversation) {
        var currentDate = Date.now();
        conversation.doc = {
          user1: _.extend(conversation._source.user1, {
            deletedDate: currentDate
          }),
          user2: _.extend(conversation._source.user1, {
            deletedDate: currentDate
          })
        };
        itemsToSave.push(conversation);
      }
      return dbh.updateListToDB(itemsToSave);
    });
  },
  createFriendRequest: function(fromUserId, toUserId, bump1, bump2) {
    bump1.doc = {
      friendStatus: 1,
      seen: true,
    };
    bump2.doc = {
      friendStatus: 2,
      seen: true,
    };

    var inboxItem = {
      _index: "inbox",
      _type: "friendRequest",
      _id: fromUserId + toUserId,
      _source: {
        user1Id: fromUserId,
        user2Id: toUserId,
        bumpId: bump2._id,
      }
    };

    return Promise.all([
      dbh.updateListToDB([bump1, bump2]),
      dbh.saveObjectToDB(inboxItem),
      dbh.fetchObject(fromUserId, "users", "user").then(function(fromUser) {
        // Don't wait for notification
        notificationsUtils.sendAddFriendNotification(fromUser, toUserId, "add_friend");
        return Promise.resolve({});
      })
    ]);
  },
  // fromUserId is the id of the user that made the post request.
  acceptFriendRequest: function(fromUserId, toUserId, bump1, bump2, inboxItemId) {
    // If inboxItemId is availabl use it. Else do the query by the two user ids.
    return dbh.fetchObject(toUserId + fromUserId, "inbox", "friendRequest")
      .then(function(inboxItem) {
        if (!inboxItem._source) {
          return Promise.reject("Friend request not found");
        }
        // Mark as friends
        bump1.doc = {
          friendStatus: 3,
          seen: true,
        };
        bump2.doc = {
          friendStatus: 3,
          seen: true,
        };
        // Hide inboxItem
        inboxItem.doc = {
          hidden: true
        };
        return Promise.all([
          dbh.updateListToDB([bump1, bump2, inboxItem]),
          dbh.fetchObject(fromUserId, "users", "user").then(function(fromUser) {
            // Don't wait for notification to send
            notificationsUtils.sendAddFriendNotification(fromUser, toUserId, "accept_friend");
            return Promise.resolve({});
          })
        ]);
      });
  }
};
module.exports = AddFriend;
