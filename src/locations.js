var couchbase = require('couchbase');
var cluster = new couchbase.Cluster('127.0.0.1');
var locationBucket = cluster.openBucket('location');
var coldStorageBucket = cluster.openBucket('cold_storage');

var ViewQuery = couchbase.ViewQuery;

var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var geolib = require('geolib');

var LOCATION_DIFFERENCE = 50;
// 10 minutes overlap to consider that two locations intersect in time
var TIME_OFFSET = 10 * 60000;
// The length of the bounding box half edge(radius) in degrees
// http://msi.nga.mil/MSISiteContent/StaticFiles/Calculators/degree.html
// 1 degree = 110575m
var BBOX_EDGE = 0.001356545;  // 150m
// var BBOX_EDGE = 0.0007234908433;  // 80m

var USERID_TO_LOCID = "userid_to_locid";

var EXPIRATION = {
  expiry: 2 * 24 * 3600
};

var Locations = {
  handleLocationsRequest: function(request, reply) {
    // console.log(request.payload);
    var currentUserId = request.payload.userId;
    var locations = request.payload.locations;

    this.processLocations(locations, currentUserId).bind(this).then(function(locations) {
        reply({
          locations: locations
        });
      }, function(error) {
        reply(error);
      });
  },

  getLocationsForUser: function(request, reply) {
    var userId = request.query.userId;
    var timeStart = request.query.timeStart;
    var timeEnd = request.query.timeEnd;
    var pw = request.query.pw;
    
    // Verify pw
    if (pw !== "4loc4") {
      reply({locations: []});
      return ;
    }

    this.getLocationsForUserBetweenDates(userId, timeStart, timeEnd).bind(this).then(function(locations) {
      reply({
        locations: locations
      });
    }, function(error) {
      reply(error);
    });
  },

  getLocationsForUserBetweenDates: function(userId, timeStart, timeEnd) {
    var locationsForUserQuery = this.getLocationsForUserQuery(userId, timeStart, timeEnd);

    return dbh.executeQuery(locationsForUserQuery, coldStorageBucket).bind(this).then(function(data) {
      var locationIds = _.pluck(data, "id");
      // locationIds = [];
      return dbh.fetchMultiObjects(locationIds, coldStorageBucket);
    }).then(function(locations) {
      locations = _.sortBy(locations, "timeStart");
      locations.reverse();
      return Promise.resolve(locations);
    });
  },

  /*
  [
    {
      location: {...},
      nearbyLocations: [ {..}, {..}],
    }
    ,
    ...
  ]
  */
  processLocations: function(locations, currentUserId) {
    locations = this.filterAndFixBadLocations(locations);
    locations = _.sortBy(locations, "time");
    locations = this.mapLocationsToDBModel(locations, currentUserId);
    // return this.fetchLatestLocation(currentUserId).bind(this).then(function(latestLocation) {
    //   return this.getLocationsNearSingleLocation(latestLocation);
    // });
    return this.fetchLatestLocation(currentUserId).bind(this).then(function(latestLocation) {
        locations = this.compressLocations(locations, latestLocation);
        return Promise.resolve();
      })
      .then(function() {
        return this.getLocationsNearLocations(locations);
        // return Promise.resolve([]);
      })
      .then(function(locationsNearLocations) {
        console.log("near loc: " + locationsNearLocations.length);
        return [locationsNearLocations, this.saveLocations(locations, currentUserId)];
      })
      .get(0);
  },

  saveLocations: function(locations, userId) {
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    // Save locations after getLocationsNearLocations, because we need the 'processed' flag set
    var saveLocationsPromise = dbh.saveListToDB(locations, "Location", EXPIRATION, locationBucket);

    // Save locations to cold storage too
    var coldSaveLocationsPromise = dbh.saveListToDB(locations, "Location", {}, coldStorageBucket);
    // var coldSaveLocationsPromise = Promise.resolve([]);

    // Now we already assigned ids to locations objects, even though the list is not saved yet
    // So we can safely get the id of the last location
    var lastLocationId = _.last(locations)["objectId"];
    var savePointerToLastLocation = dbh.savePointerToDB(USERID_TO_LOCID, userId, lastLocationId, EXPIRATION, locationBucket);
    return Promise.all(saveLocationsPromise, coldSaveLocationsPromise, savePointerToLastLocation);
  },

  /*
  [
    {
      location: {...},
      nearbyLocations: [ {..}, {..}],
    }
    ,
    ...
  ]
  */
  getLocationsNearLocations: function(locations) {
    // console.log(JSON.stringify(locations));
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    var tasks = _.map(locations, function(location) {
      return this.getLocationsNearSingleLocation(location);
    }.bind(this));

    return Promise.settle(tasks).bind(this).then(function(results) {
      var locationsNearLocations = [];
      _.each(results, function(result) {
        if (result.isFulfilled()) {
          locationsNearLocations.push(result.value());
        }
      });
      return Promise.resolve(locationsNearLocations);
    });
  },

  getLocationsForUserQuery: function(userId, timeStart, timeEnd) {
    var locationsForUserQuery = ViewQuery.from("locations", "location_by_userid");

    var startkey = '["' + userId + '", ' + timeStart + ']';
    var endkey = '["' + userId + '", ' + timeEnd + ']';

    locationsForUserQuery.custom({
      startkey: startkey,
      endkey: endkey,
    });
    console.log("xxx: " + JSON.stringify(locationsForUserQuery));
    return locationsForUserQuery;
  },

  getSpatialQuery1: function(location) {
    var nearLocationQuery = ViewQuery.fromSpatial("spatial", "location_space_time");

    var startRange = [location.latitude - BBOX_EDGE,
                      location.longitude - BBOX_EDGE,
                      location.timeStart - TIME_OFFSET];
    var endRange = [location.latitude + BBOX_EDGE,
                    location.longitude + BBOX_EDGE,
                    location.timeEnd + TIME_OFFSET];
    nearLocationQuery.custom({
      start_range: JSON.stringify(startRange),
      end_range: JSON.stringify(endRange),
    });
    return nearLocationQuery;
  },

  // getSpatialQuery2: function(location) {
  //   var nearLocationQuery = ViewQuery.fromSpatial("spatial", "location_space_time2");

  //   var startRange = [location.timeStart - TIME_OFFSET,
  //                     location.latitude - BBOX_EDGE,
  //                     location.longitude - BBOX_EDGE];
  //   var endRange = [location.timeEnd + TIME_OFFSET,
  //                   location.latitude + BBOX_EDGE,
  //                   location.longitude + BBOX_EDGE];
  //   nearLocationQuery.custom({
  //     start_range: JSON.stringify(startRange),
  //     end_range: JSON.stringify(endRange),
  //   });
  //   return nearLocationQuery;
  // },

  /* If promise is fulfilled, add {processed: true} to location
    {
      location: {},
      nearbyLocations: []
    }
  */
  getLocationsNearSingleLocation: function(location) {
    var nearLocationQuery = this.getSpatialQuery1(location);

    return dbh.executeQuery(nearLocationQuery, locationBucket).bind(this).then(function(data) {
      var locationIds = _.map(data, function(location) {
          return location.id;
      });
      // locationIds = [];
      return dbh.fetchMultiObjects(locationIds, locationBucket);
    }).then(function(nearbyLocations) {
      // Remove nearby locations that belong to current user
      console.log("nearby before: " + nearbyLocations.length);
      nearbyLocations = this.filterLocationsFromCurrentUserId(nearbyLocations, location["userId"]);
      console.log("nearby after: " + nearbyLocations.length);
      var object = {
        location: location,
        nearbyLocations: nearbyLocations
      };
      return Promise.resolve(object);
    });
  },

  filterLocationsFromCurrentUserId: function(locations, currentUserId) {
    return _.filter(locations, function(location) {
      return location["userId"] !== currentUserId;
    });
  },

  mapLocationsToDBModel: function(locations, userId) {
    return _.map(locations, function(location) {
      return {
        latitude:       location["latitude"],
        longitude:      location["longitude"],
        timeStart:      location["time"],
        timeEnd:        location["time"],
        timeSpent:      0,
        accuracy:       location["accuracy"],
        userId:         userId,
      };
    });
  },

  compressLocations: function(locations, latestLocation) {
    console.log("locs: " + JSON.stringify(locations.length));
    console.log("last: " + latestLocation);
    var compressedLocations = [];

    if (locations.length === 0) {
      return compressedLocations;
    }
    if (latestLocation) {
      locations = this.filterOlderLocations(locations, latestLocation["timeStart"]);
      if (locations.length === 0) {
        return compressedLocations;
      }
      // If previous location ends after the new location start, it means that the previous location had a bigger
      // expiration than needed. In that case, we change previous location timeEnd so that it won't overlap with
      // the new location.
      var newLocation = _.first(locations);
      if (newLocation["timeStart"] < latestLocation["timeEnd"]) {
        latestLocation["timeEnd"] = newLocation["timeStart"];
        latestLocation["timeSpent"] = latestLocation["timeEnd"] - latestLocation["timeStart"];
      }
    } else {
      latestLocation = _.first(locations);
    }
    compressedLocations.push(latestLocation);

    _.each(locations, function(newLocation) {
      var newLocationPoint = {latitude: newLocation["latitude"], longitude: newLocation["longitude"]};
      var latestLocationPoint = {latitude: latestLocation["latitude"], longitude: latestLocation["longitude"]};

      // If new location is almost the same as previous location => merge the new one into the previous one
      if (geolib.getDistance(newLocationPoint, latestLocationPoint) < LOCATION_DIFFERENCE) {
        latestLocation["timeEnd"] = newLocation["timeEnd"];
        latestLocation["timeSpent"] = latestLocation["timeEnd"] - latestLocation["timeStart"];
      } else {
        latestLocation = newLocation;
        compressedLocations.push(latestLocation);
      }
    });

    // It's safe to supose that the user will stay here for the next x hours
    // until he uploads a new locaiton. In that case we'll shrink that time interval.
    // Add 2 hours offset to the latest location.
    latestLocation["timeEnd"] = latestLocation["timeEnd"] + 2 * 3600000;
    latestLocation["timeSpent"] = latestLocation["timeSpent"] + 2 * 3600000;
    console.log("comp locs: " + compressedLocations.length);
    return compressedLocations;
  },

  fetchLatestLocation: function(userId) {
    return dbh.fetchPointer(USERID_TO_LOCID, userId, locationBucket);
  },

  filterOlderLocations: function(locations, olderThan) {
    // Filter out old locations
    return _.filter(locations, function(location) {
      var timeStart = location["timeStart"];
      return !(timeStart < olderThan);
    });
  },

  filterAndFixBadLocations: function(locations) {
    // Filter out bad locations
    return _.filter(locations, function(location) {
      var latitude = location["latitude"];
      var longitude = location["longitude"];

      // Fix time issues
      if (!location["time"]) {
        // If location doesn't have a time, set current time
        location["time"] = Date.now();
      } else if (location["time"] > Date.now() + 3600000) {
        // If locations is more than 1 hour into the future, set current time
        location["time"] = Date.now();
      }
      return -90 < latitude && latitude < 90 && -90 < longitude && longitude < 90;
    });
  },
};

module.exports = Locations;