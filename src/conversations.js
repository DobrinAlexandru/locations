var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var requestLib = Promise.promisify(require("request"));

var notificationsUtils = require('./notifications');
var conversationsUtils = require('./conversations');
var utils = require('./utils');

var Conversations = {
  loadConversations: function(payload) {
    var userId = payload.currentUserId;
    var skip = payload.skip;
    var limit = payload.limit;
    return dbh.loadConversations(userId, null, skip, limit).bind(this).then(function(conversations) {
      conversations = conversations.hits.hits;
      var otherUsersIds = utils.getOtherUsersIds(userId, conversations);
      console.log("1 " + conversations.length);
      if (conversations.length === 0) {
        return Promise.all([
         Promise.resolve([]),
         Promise.resolve({docs: []}),
         Promise.resolve({hits: {hits: []}})]);
      }
      // Load users & bumps & attach to each conversation
      return Promise.all([
        Promise.resolve(conversations),
        dbh.fetchMultiObjects(otherUsersIds, "users", "user"),
        dbh.loadBumps(userId, otherUsersIds, false, 0, otherUsersIds.length)
      ]);
    }).spread(function(conversations, users, bumps) {
      users = users.docs;
      bumps = bumps.hits.hits;
      console.log("1.1 " + bumps.length);
      return Promise.resolve(this.attachUsersAndBumps(userId, conversations, users, bumps));
    });;
  },
  loadMessages: function(payload) {
    var userId = payload.currentUserId;
    var otherUserId = payload.otherUserId;
    var skip = payload.skip;
    var limit = payload.limit;
    var newerThan = payload.newerThanDate;
    return dbh.loadMessages(userId, otherUserId, newerThan, skip, limit)
      .then(function(messages) {
        messages = messages.hits.hits;
        // Sort messages back in ascending order
        messages = _.sort(messages, function(message) {
          return message._source.createdAt;
        });
        return Promise.resolve(messages);
      });
  },
  // TODO transform output
  sendMessage: function(payload) {
    var fromUserId = payload.fromUserId;
    var toUserId = payload.toUserId;
    var msg = payload.msg;
    var msgDate = Date.now();

    if (_.contains(["viGMhiHFBh"], fromUserId)) {
      return Promise.reject("Banned");
    }
    
    var fromUser, toUser;
    return dbh.fetchMultiObjects([fromUserId, toUserId], "users", "user").bind(this).then(function(users) {
      users = users.docs;
      console.log("1 " + users.length);
      fromUser = users[0];
      toUser = users[1];
      return this.createOrUpdateConversation(fromUser, toUser, msg, msgDate);
    }).then(function() {
      return this.createMessage(fromUser, toUser, msg, msgDate);
    }).then(function(savedMsg) {
      // Don't wait for notification
      notificationsUtils.sendMsgNotification(fromUser, toUser._id, msg);
      return Promise.resolve(savedMsg);
    });
  },
  markConversationAsRead: function(payload) {
    var conversationId = payload.conversationId;
    var userId = payload.currentUserId;
    return dbh.fetchObject(conversationId, "conversations", "conversation").then(function(conversation) {
      if (!conversation._source) {
        return Promise.reject("Conversation not found");
      } else {
        if (userId === conversation._source.user1.userId) {
          conversation.doc = {
            user1: _.extend(conversation._source.user1, {
              msgsUnread: 0
            })
          };
        } else {
          conversation.doc = {
            user2: _.extend(conversation._source.user2, {
              msgsUnread: 0
            })
          };
        }
        return dbh.updateObjectToDb(conversation);
      }
    });
  },
  // TODO test
  deleteConversation: function(payload) {
    var conversationId = payload.conversationId;
    var userId = payload.currentUserId;
    return dbh.fetchObject(conversationId, "conversations", "conversation").bind(this).then(function(conversation) {
      if (!conversation._source) {
        return Promise.reject("Conversation not found");
      } else {
        if (userId === conversation._source.user1.userId) {
          conversation.doc = {
            user1: _.extend(conversation._source.user1, {
              deleted: true
            })
          };
        } else {
          conversation.doc = {
            user2: _.extend(conversation._source.user2, {
              deleted: true
            })
          };
        }
        return dbh.updateObjectToDb(conversation);
      }
    });
  },

  attachUsersAndBumps: function(userId, conversations, users, bumps) {
    // User hash
    var withUsersHash = _.object(_.map(users, function(user) {
      return [user._id, user];
    }));
    // Bumps hash
    var withBumpsHash = _.object(_.map(bumps, function(bump) {
      return [bump._source.user2.userId, bump];
    }));
    var results = _.map(conversations, function(conversation) {
      var otherId = userId === conversation._source.user1.userId ? 
                            conversation._source.user2.userId :
                            conversation._source.user1.userId;
      return {
        conversation: conversation,
        user: withUsersHash[otherId],
        bump: withBumpsHash[otherId]
      };
    });
    // Filter out bad conversations
    results = _.filter(results, function(obj) {
      console.log("4.1 " + obj.user);
      console.log("4.2 " + obj.bump);
      return !!(obj.user && obj.bump);
    });
    return results;
  },
  createOrUpdateConversation: function(fromUser, toUser, msg, msgDate) {
    return dbh.fetchObject(utils.keys(fromUser._id, toUser._id), "conversations", "conversation").bind(this)
      .then(function(conversation) {
        console.log("2 " + JSON.stringify(conversation));
        if (conversation._source) {
          return this.updateConversation(conversation, fromUser, msg, msgDate);
        } else {
          return this.createConversation(fromUser, toUser, msg, msgDate)
        }
      });
  },
  createConversation: function(fromUser, toUser, msg, msgDate) {
    var conversation = {
      _index: "conversations",
      _type: "conversation",
      _id: utils.keys(fromUser._id, toUser._id),
      _source: {
        user1: {
          userId: fromUser._id,
          firstName: fromUser._source.firstName,
          msgsSent: msg ? 1 : 0,
          msgsUnread: msg ? 0 : 1,
          // deleted: false,
        },
        user2: {
          userId: toUser._id,
          firstName: toUser._source.firstName,
          msgsSent: 0,
          msgsUnread: 0,
          // deleted: false,
        },
        totalMsgs: 1,
        lastMsg: {
          text: msg,
          time: msgDate
        }
      }
    };
    return dbh.saveObjectToDB(conversation);
  },
  updateConversation: function(conversation, fromUser, msg, msgDate) {
    var update = {
      lastMsg: {
        text: msg,
        time: msgDate
      },
      totalMsgs: (conversation._source.totalMsgs || 0) + 1
    };
    
    var user1 = conversation._source.user1;
    var user2 = conversation._source.user2;
    if (fromUser._id === user1.userId) {
      update.user1 = _.extend(user1, {
        msgsSent: (user1.msgsSent || 0) + 1,
      });
      if (user1.deleted) update.user1.deleted = false;

      update.user2 = _.extend(user2, {
        msgsUnread: (user2.msgsUnread || 0) + 1,
      });
      if (user2.deleted) update.user2.deleted = false;
    } else {
      update.user2 = _.extend(user2, {
        msgsSent: (user2.msgsSent || 0) + 1,
      });
      if (user2.deleted) update.user2.deleted = false;

      update.user1 = _.extend(user1, {
        msgsUnread: (user1.msgsUnread || 0) + 1,
      });
      if (user1.deleted) update.user1.deleted = false;
    }
    conversation.doc = update;
    return dbh.updateObjectToDb(conversation);
  },
  createMessage: function(fromUser, toUser, msg, msgDate) {
    var message = {
      _index: "messages",
      _type: "message",
      _source: {
        createdAt: msgDate,
        user1: {
          userId: fromUser._id,
          firstName: fromUser._source.firstName,
          fbid: fromUser._source.fbid
        },
        user2: {
          userId: toUser._id,
          firstName: toUser._source.firstName,
          fbid: toUser._source.fbid
        },
        msg: msg,
        type: 0
      }
    };
    return dbh.saveObjectToDB(message);
  }
};
module.exports = Conversations;

