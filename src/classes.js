
exports.WUser = function(user) {
  this.objectId = user._id;
  this.name = user._source.firstName;
  this.facebookId = user._source.fbid;
  this.gender = user._source.gender === 1 ? "female" :
                    (user._source.gender === 2 ? "male" : null);
  this.fbPhotos = user._source.fbPhotos;
  this.profileUrl = user._source.profileUrl;
  if (user._source.location) {
    this.latitude = user._source.location.lat;
    this.longitude = user._source.location.lon;
  }
  this.pictureRatio = user._source.pictureRatio;
  this.education = user._source.education;

  this.feedLoadedTime = user._source.feedLoadedTime;
  this.interestedIn = user._source.genderInt;
  if (user._source.birthday) {
    this.age = parseInt((Date.now() - user._source.birthday) / 31536000000);
  }
  this.fbFriends = user._source.fbFriends;
}

exports.WBump = function(bump, user, conversation) {
  this.objectId = bump._id;
  this.updatedAt = bump._source.updatedAt;
  this.createdAt = bump._source.createdAt;
  this.lastIncrDate = bump._source.updatedAt;
  this.userId1 = bump._source.user1.userId;
  this.userId2 = bump._source.user2.userId;
  this.userName1 = bump._source.user1.firstName;
  this.userName2 = bump._source.user2.firstName;
  this.bumps = bump._source.nrBumps;
  if (bump._source.location) {
    this.latitude = bump._source.location.lat;
    this.longitude = bump._source.location.lon;
  }
  this.time = bump._source.locationTime;
  this.friendStatus = bump._source.friendStatus === 1 ? "request_sent" :
                          (bump._source.friendStatus === 2 ? "request_pending" : 
                                (bump._source.friendStatus === 3 ? "friends" : null));

  if (user){
    this.userFbId2 = user._source.fbid;
    this.user = new exports.WUser(user);
  }
  if (conversation) this.conversation = new exports.WConversation(conversation);
}

exports.WConversation = function(conversation, user, bump) {
  this.objectId = conversation._id;
  this.userId1 = conversation._source.user1.userId;
  this.userId2 = conversation._source.user2.userId;

  this.lastMsgDate = conversation._source.lastMsg.time;
  this.lastMsg = conversation._source.lastMsg.text;

  this.unreadFromUser1 = conversation._source.user2.msgsUnread;
  this.unreadFromUser2 = conversation._source.user1.msgsUnread;

  this.msgsFromUser1 = conversation._source.user1.msgsSent;
  this.msgsFromUser2 = conversation._source.user2.msgsSent;
  this.totalMsgs = conversation._source.totalMsgs;

  this.deletedByUser1 = conversation._source.user1.deleted;
  this.deletedByUser2 = conversation._source.user2.deleted;

  if (user) this.user = new exports.WUser(user);
  if (bump) {
    this.bump = new exports.WBump(bump);
    if (user) this.bump.userFbId2 = user._source.fbid;
  }
}

exports.WMessage = function(message) {
  this.objectId = message._id;
  this.fromId = message._source.user1.userId;
  this.fromName = message._source.user1.firstName;
  this.fromFbId = message._source.user1.fbid;
  this.toId = message._source.user2.userId;
  this.toName = message._source.user2.firstName;
  this.msgText = message._source.msg;
  this.updatedAt = message.createdAt;
}

exports.PMessage = function(message) {
  var Class = Parse.Object.extend("Message");
  var obj = new Class();
  obj.id = message.objectId;
  obj.set("from_id", message.fromId);
  obj.set("fb_id", message.fromFbId);
  obj.set("from_name", message.fromName);
  obj.set("to_id", message.toId);
  obj.set("msg", message.msgText);
  obj.updatedAt = obj.createdAt = new Date(message.createdAt);
  obj.dirty = function() { return false; };
  return obj;
}

exports.WInbox = function(bump, user1, user2) {
  this.objectId = user2._id;
  this.type = "friend_request";
  this.fromUserId = user2._id;
  this.toUserId = user1._id;
  this.bumpId = bump._id;
  this.fromUser = new exports.WUser(user2);
  this.toUser = new exports.WUser(user1);
  this.bump = new exports.WBump(bump);
  if (user2) this.bump.userFbId2 = user2._source.fbid;
}


