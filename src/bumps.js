var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var requestLib = Promise.promisify(require("request"));

var notificationsUtils = require('./notifications');

var MAX_FAKE_USERS = 3;

var Bumps = {
  processLocationsAndCreateOrUpdateBumps: function(payload) {
    // This function is called with small, medium and large radius until we created enough bumps
    var retryPromiseFunction = function(radius) {
      payload.radius = radius;
      return requestLib({
        url: 'http://78.46.230.72:8001/locations',
        method: 'POST',
        json: true,
        body: payload
      }).bind(this).then(function(locations) {
        // Try to create bumps with the biggest radius and add fake bumps if nothing was found
        var tryAddFakeBumps = (radius === 2);
        return this.createOrUpdateBumps(payload.userId, locations, tryAddFakeBumps);
      });
    };
    
    return retryPromiseFunction(0).then(function(result) {
      return result.bumpsToAdd <= 1 ? Promise.resolve(result) : retryPromiseFunction(1);
    }).then(function(result) {
      return result.bumpsToAdd <= 1 ? Promise.resolve(result) : retryPromiseFunction(2);
    });
  },
  loadNewsFeed: function(payload) {

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
        // Group users by id
        usersById = _.object(_.map(users, function(user) {
          return [user._id, user];
        }));
        return [
          this.createOneWayBumps(userId, otherUsersIds, usersById, locationsByUser, false),
          this.createOneWayBumps(userId, otherUsersIds, usersById, locationsByUser, true),
        ];
      }).then(function(bumps) {
        // Save all bumps, Send notification, Increment nrBumps on user
        return this.saveBumpsAndSendNotifications(bumps, userId, usersById);
      }).then(function() {
        // Add fake users if nr of bumps is low
        return this.addFakeBumpsIfNeeded(userId, usersById, _.last(locations), tryAddFakeBumps);
      });
  },

  saveBumpsAndSendNotifications: function(bumps, userId, usersById) {
    var user = usersById[userId];
    var createdBumps = bumps[0][0].concat(bumps[1][0]);
    // var updatedBumps = bumps[0][1].concat(bumps[1][1]);
    var allBumps = _.flatten(bumps);

    var promises = [];
    // Increment nrBumps on user
    if (bumps[0][0].length > 0 && user._source.nrBumps < 10) {
      promises.push(dbh.increment(user, "nrBumps", bumps[0][0].length));
    }
    // Save all bumps
    if (allBumps.length > 0) {
      promises.push(dbh.saveListToDB(allBumps));
    }
    // Send notification
    if (createdBumps.length > 0) {
      notificationsUtils.sendNewBumpsNotification(createdBumps, usersById)
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
    var nrFakeUsersToPick = MAX_FAKE_USERS - user._source.nrBumps;
    // Return number of bumps to add, even though we didn't created bumps using fake users
    // This way, at a higher level, we'll search for real locations at an increased radius
    if (!tryAddFakeBumps || nrFakeUsersToPick <= 0) {
      return Promise.resolve({bumpsToAdd: nrFakeUsersToPick});
    }

    return dbh.pickAvailableFakeUsers(user, nrFakeUsersToPick).then(function(fakeUsers) {
      fakeUsers = fakeUsers.hits.hits;
      var fakeNearbyLocations = _.map(fakeUsers, function(fakeUser) {
        var fakeLocation = _.clone(lastLocation);
        fakeLocation._source.userId = fakeUser._id;
        return fakeLocation;
      });
      var fakeLocations = [{
        location: lastLocation,
        nearbyLocations: fakeNearbyLocations
      }];

      // Create modify docs
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
    return dbh.loadBumpsBetweenIds(userId, otherUsersIds, reverse).then(function(bumps) {
      var existingsBumps = bumps.hits.hits;
      // Optimisation since Date.now() is expensive
      var currentTime = Date.now();

      // Update existings bumps
      var data = this.updateExistingsBumps(existingsBumps, userId, usersById, locationsByUser, currentTime, reverse);
      var updatedBumps = data.updatedBumps;

      // Get users ids for witch we create a new bump
      var usersIdsWithoutBumps = _.difference(otherUsersIds, data.userIdsWithBumps);
      // Create new bumps
      var createdBumps = this.createNewBumps(usersIdsWithoutBumps, userId, usersById, locationsByUser, currentTime, reverse);
      
      return [createdBumps, updatedBumps];
    });
  },

  updateExistingsBumps: function(existingsBumps, userId, usersById, locationsByUser, currentTime, reverse) {
    var halfDayAgo = Date.now() - 12 * 3600000;
    var updatedBumps = [];
    var userIdsWithBumps = [];
    _.each(existingsBumps, function(bump) {
      var otherUserId = reverse ? bump._source.user1.userId : bump._source.user2.userId;
      var user1 = !reverse ? usersById[userId] : usersById[otherUserId];
      var user2 = reverse ? usersById[userId] : usersById[otherUserId];
      // Check if enough time passed to update bump
      if (!bump._source.updatedAt || bump._source.updatedAt < halfDayAgo) {
        this.updateExistingBump(bump, user1, user2, locationsByUser[otherUserId], currentTime, reverse);
        updatedBumps.push(bump);
      }
      // Keep users ids from existing bumps
      userIdsWithBumps.push(otherUserId);
    });
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
    });
    return createdBumps;
  },

  createNewBump: function(user1, user2, locationsPairs, currentTime, reverse) {
    var bump = {
      _index: "bumps",
      _type: "bump",
      // _id: uuid.v1(),
      _source: {
        createdAt: currentTime,
        updatedAt: currentTime,
        visible: true,
        seen: false
      }
    };
    return updateExistingBump(bump, user1, user2, locationsPairs, currentTime, reverse);
  },
  updateExistingBump: function(bump, user1, user2, locationsPairs, currentTime, reverse) {
    bump._source.user1 = _.pick(user1, "userId");
    bump._source.user2 = _.pick(user2, "userId", "nameShort", "fbid", "birthday", "ageIntMin", "ageIntMax", "gender", "genderInt");
    bump._source.updatedAt = currentTime;
    bump._source.nrBumps = (bump._source.nrBumps || 0) + 1;

    var latestLocation = _.max(locationsPairs, function(pair) {
      return !reverse ? pair.location._source.timeStart : pair.nearbyLocation._source.timeStart;
    });

    bump._source.locationTime = latestLocation._source.timeStart;
    bump._source.location = latestLocation._source.location;
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
  }
};

module.exports = Bumps;