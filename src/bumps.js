var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var requestLib = Promise.promisify(require("request"));

var notificationsUtils = require('./notifications');
var utils = require('./utils');

var MAX_FAKE_USERS = 3;

var Bumps = {
  processLocationsAndCreateOrUpdateBumps: function(payload) {
    // This function is called with small, medium and large radius until we created enough bumps
    var retryPromiseFunction = function(radius) {
      payload.radius = radius;
      return requestLib({
        url: utils.C.LOCATIONS_IP + '/locations',
        method: 'POST',
        json: true,
        body: payload
      }).bind(this).then(function(locations) {
        console.log("3 " + radius);
        locations = locations[1].locations;
        // Try to create bumps with the biggest radius and add fake bumps if nothing was found
        return this.createOrUpdateBumps(payload.userId, locations, (radius === 0));
      });
    }.bind(this);
    
    return retryPromiseFunction(0);
    // .then(function(result) {
    //   return result.bumpsToAdd <= 1 ? Promise.resolve(result) : retryPromiseFunction(1);
    // });
    // .then(function(result) {
    //   return result.bumpsToAdd <= 1 ? Promise.resolve(result) : retryPromiseFunction(2);
    // })
  },
  loadNewsFeed: function(payload) {
    var userId = payload.currentUserId;
    var skip = payload.skip;
    var limit = payload.limit;
    var seen = payload.seen;
    return dbh.fetchObject(userId, "users", "user").bind(this).then(function(user) {
      user.doc = {
        "feedLoadedTime": Date.now()
      };
      // Update user without blocking
      dbh.updateObjectToDb(user);
      return dbh.loadBumps({user: user, filters: true, sort: true, seen: seen}, false, skip, limit)
    }).then(function(bumps) {
      bumps = bumps.hits.hits;
      console.log("1 " + bumps.length);
      var otherUsersIds = utils.getOtherUsersIds(userId, bumps);
      if (bumps.length === 0) {
        return Promise.all([
          Promise.resolve([]),
          Promise.resolve({docs: []}),
          Promise.resolve({hits: {hits: []}})
        ]);
      }
      // Load users & bumps & attach to each conversation
      return Promise.all([
        Promise.resolve(bumps),
        dbh.fetchMultiObjects(otherUsersIds, "users", "user"),
        dbh.loadConversations(userId, otherUsersIds, 0, otherUsersIds.length)
      ]);
    }).spread(function(bumps, users, conversations) {
      users = users.docs;
      conversations = conversations.hits.hits;
      console.log("2.1 " + userId);
      console.log("2.2 " + bumps.length);
      return Promise.resolve(this.attachUsersAndConversations(userId, bumps, users, conversations));
    });
  },
  markBumpAsSeen: function(payload) {
    var userId1 = payload.userId1;
    var userId2 = payload.userId2;
    return dbh.fetchObject(userId1 + userId2, "bumps", "bump").then(function(bump) {
      bump.doc = {
        seen: true
      };
      return dbh.updateObjectToDb(bump);
    });
  },
  /*
    returns Promise {
      bumpsToAdd: x
    }
  */
  createOrUpdateBumps: function(userId, locations, tryAddFakeBumps) {
    var locationsByUser = this.getLocationsByUser(locations);
    var otherUsersIds = _.keys(locationsByUser);
    var usersIds = otherUsersIds.concat(userId);
    var usersById = {};
    // Fetch users
    return dbh.fetchMultiObjects(usersIds, "users", "user").bind(this).then(function(users) {
        users = users.docs;
        console.log("5 " + users.length);
        // Group users by id
        usersById = _.object(_.map(users, function(user) {
          return [user._id, user];
        }));
        // console.log("5.1 " + JSON.stringify(usersById));
        // TODO handle empty lists
        return Promise.all([
          this.createOneWayBumps(userId, otherUsersIds, usersById, locationsByUser, false),
          this.createOneWayBumps(userId, otherUsersIds, usersById, locationsByUser, true),
        ]);
      }).then(function(bumps) {
        // Save all bumps, Send notification, Increment nrBumps on user
        return this.saveBumpsAndSendNotifications(bumps, userId, usersById);
      }).then(function() {
        // Add fake users if nr of bumps is low
        return this.addFakeBumpsIfNeeded(userId, usersById, _.last(locations), tryAddFakeBumps);
      });
  },

  saveBumpsAndSendNotifications: function(bumps, userId, usersById) {
    console.log("7 " + bumps);
    var user = usersById[userId];
    var createdBumps = bumps[0][0].concat(bumps[1][0]);
    var updatedBumps = bumps[0][1].concat(bumps[1][1]);
    var allBumps = _.flatten(bumps);

    var promises = [];
    // Increment nrBumps on user
    if (bumps[0][0].length > 0 && (user._source.nrBumps || 0) < 10) {
      console.log("7.1 " + bumps[0][0].length);
      promises.push(dbh.increment(user, "nrBumps", bumps[0][0].length));
    }
    if (updatedBumps.length > 0) {
      // Update bumps
      promises.push(dbh.updateListToDB(updatedBumps));
    }
    if (createdBumps.length > 0) {
      // Save created bumps
      promises.push(dbh.saveListToDB(createdBumps));
      // Send notification
      notificationsUtils.sendNewBumpsNotification(createdBumps, usersById);
    }
    return promises;
  },
  /*
    returns Promise {
      bumpsToAdd: x
    }
  */
  addFakeBumpsIfNeeded: function(userId, usersById, lastLocation, tryAddFakeBumps) {
    var user = usersById[userId];
    var nrFakeUsersToPick = MAX_FAKE_USERS - (user._source.nrBumps || 0);
    // Return number of bumps to add, even though we didn't created bumps using fake users
    // This way, at a higher level, we'll search for real locations at an increased radius
    console.log("10 " + nrFakeUsersToPick);
    if (!tryAddFakeBumps || nrFakeUsersToPick <= 0) {
      return Promise.resolve({bumpsToAdd: nrFakeUsersToPick});
    }

    return dbh.pickAvailableFakeUsers(user, nrFakeUsersToPick).bind(this).then(function(fakeUsers) {
      fakeUsers = fakeUsers.hits.hits;
      if (fakeUsers.length <= 0) {
        console.log("10.1 no fake users found");
        return Promise.resolve([{bumpsToAdd: nrFakeUsersToPick}]);
      }
      console.log("10.2 fake bumps found " + fakeUsers.length);
      var fakeNearbyLocations = _.map(fakeUsers, function(fakeUser) {
        var fakeLocation = _.clone(lastLocation.location);
        fakeLocation._source.userId = fakeUser._id;
        return fakeLocation;
      });
      var fakeLocations = [{
        location: lastLocation.location,
        nearbyLocations: fakeNearbyLocations
      }];

      // update lastTimeFake for used fake users
      var currentTime = Date.now();
      var fakeUsersUpdates = _.map(fakeUsers, function(fakeUser) {
        var update = _.pick(fakeUser, "_index", "_type", "_id");
        update.doc = {
          lastTimeFake: currentTime
        };
        return update;
      });
      return [
        // Create or update bumps using fake locations with fake users
        this.createOrUpdateBumps(userId, fakeLocations, false),
        // Update fake users
        dbh.updateListToDB(fakeUsersUpdates)
      ];
    }).get(0);
  },

  // Create bumps between userId and otherUsersIds, using the "reverse" variable as a way a -> b or a <- b
  createOneWayBumps: function(userId, otherUsersIds, usersById, locationsByUser, reverse) {
    console.log("6 " + JSON.stringify(otherUsersIds));
    return dbh.loadBumps({userId: userId, otherUsersIds: otherUsersIds, hidden: true}, reverse, 0, otherUsersIds.length).bind(this).then(function(bumps) {
      var existingsBumps = bumps.hits.hits;
      console.log("6.2 existings bumps " + existingsBumps.length);
      // Optimisation since Date.now() is expensive
      var currentTime = Date.now();
      // Update existings bumps
      var data = this.updateExistingsBumps(existingsBumps, userId, usersById, locationsByUser, currentTime, reverse);
      var updatedBumps = data.updatedBumps;

      // Get users ids for witch we create a new bump
      var usersIdsWithoutBumps = _.difference(otherUsersIds, data.userIdsWithBumps);
      console.log("9.0 ids without bumps " + JSON.stringify(usersIdsWithoutBumps));
      // Create new bumps
      var createdBumps = this.createNewBumps(usersIdsWithoutBumps, userId, usersById, locationsByUser, currentTime, reverse);
      console.log("9 created " + createdBumps.length);
      console.log("9 updated " + updatedBumps.length);

      return Promise.resolve([createdBumps, updatedBumps]);
    });
  },

  updateExistingsBumps: function(existingsBumps, userId, usersById, locationsByUser, currentTime, reverse) {
    var halfDayAgo = Date.now() - utils.C.DAY / 2;
    var updatedBumps = [];
    var userIdsWithBumps = [];
    _.each(existingsBumps, function(bump) {
      var otherUserId = reverse ? bump._source.user1.userId : bump._source.user2.userId;
      var user1 = !reverse ? usersById[userId] : usersById[otherUserId];
      var user2 = reverse ? usersById[userId] : usersById[otherUserId];
      // Check if enough time passed to update bump
      if (bump._source.updatedAt < halfDayAgo) {
        this.updateExistingBump(bump, user1, user2, locationsByUser[otherUserId], currentTime, reverse, true);
        updatedBumps.push(bump);
      }
      // Keep users ids from existing bumps
      userIdsWithBumps.push(otherUserId);
    }.bind(this));
    return {
      updatedBumps: updatedBumps,
      userIdsWithBumps: userIdsWithBumps
    };
  },
  createNewBumps: function(usersIdsWithoutBumps, userId, usersById, locationsByUser, currentTime, reverse) {
    var createdBumps = _.map(usersIdsWithoutBumps, function(otherUserId) {
      var user1 = !reverse ? usersById[userId] : usersById[otherUserId];
      var user2 = reverse ? usersById[userId] : usersById[otherUserId];
      return this.createNewBump(user1, user2, locationsByUser[otherUserId], currentTime, reverse);
    }.bind(this));
    return createdBumps;
  },

  createNewBump: function(user1, user2, locationsPairs, currentTime, reverse) {
    var bump = {
      _index: "bumps",
      _type: "bump",
      _id: user1._id + user2._id,
      _source: {
        createdAt: currentTime,
        updatedAt: currentTime,
        seen: false
      }
    };
    return this.updateExistingBump(bump, user1, user2, locationsPairs, currentTime, reverse);
  },
  updateExistingBump: function(bump, user1, user2, locationsPairs, currentTime, reverse, treatAsUpdate) {
    var latestLocation = _.max(locationsPairs, function(pair) {
      return !reverse ? pair.location._source.timeStart : pair.nearbyLocation._source.timeStart;
    });
    latestLocation = !reverse ? latestLocation.location : latestLocation.nearbyLocation;

    var update = {
      updatedAt:    currentTime,
      nrBumps:      (bump._source.nrBumps || 0) + 1,
      locationTime: latestLocation._source.timeStart,
      location:     latestLocation._source.location,
      user1:        {
        userId: user1._id
      },
      user2:        _.extend(_.pick(user2._source, "firstName", "birthday", "ageIntMin", "ageIntMax", "gender", "genderInt"), {
        userId: user2._id
      })
    };
    
    _.extend(bump._source, update);
    if (treatAsUpdate) {
      bump.doc = update;
    }
    return bump;
  },
  getLocationsByUser: function(locations) {
    var locationsByUser = {};
    _.each(locations, function(pair) {
      _.each(pair.nearbyLocations, function(nearbyLoc) {
        var locationsGroup = locationsByUser[nearbyLoc._source.userId];
        var locationPair = {
          location: pair.location,
          nearbyLocation: nearbyLoc,
        }
        if (!locationsGroup) {
          locationsGroup = [];
          locationsByUser[nearbyLoc._source.userId] = locationsGroup;
        }
        locationsGroup.push(locationPair);
      });
    });
    return locationsByUser;
  },
  attachUsersAndConversations: function(userId, bumps, users, conversations) {
    console.log("4 ");
    // User hash
    var withUsersHash = _.object(_.map(users, function(user) {
      return [user._id, user];
    }));
    // Conversations hash
    var withConversationsHash = _.object(_.map(conversations, function(conversation) {
      var otherId = userId === conversation._source.user1.userId ? 
                            conversation._source.user2.userId :
                            conversation._source.user1.userId;
      return [otherId, conversation];
    }));
    var results = _.map(bumps, function(bump) {
      var otherId = bump._source.user2.userId;
      return {
        bump: bump,
        user: withUsersHash[otherId],
        conversation: withConversationsHash[otherId]
      };
    });
    // Filter out bad conversations
    results = _.filter(results, function(obj) {
      return !!(obj.user);
    });
    console.log("4 " + results.length);
    return results;
  },
};

module.exports = Bumps;