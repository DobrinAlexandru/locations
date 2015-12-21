var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var geolib = require('geolib');
var uuid = require('node-uuid');

var LOCATION_DIFFERENCE = 50;
var USERID_TO_LOCID = "useridToLocid";

var Locations = {
  handleLocationsRequest: function(request, reply) {
    var timerStart = Date.now();
    var currentUserId = request.payload.userId;
    var locations = request.payload.locations;
    
    // if (currentUserId !== "EIxcvQA5J6") {
    //   return;
    // }
    console.log("\n--- START --- " + currentUserId);

    this.processLocations(locations, currentUserId).bind(this).then(function(locations) {
        console.log("process: finished");
        var output = _.map(locations, function(pair) {
          return {
            location: this.getLocationFromDbModel(pair.location),
            nearbyLocations: _.map(pair.nearbyLocations, function(location) {
              return this.getLocationFromDbModel(location);
            }.bind(this))
          }
        }.bind(this));
        console.log("TIME total: " + (Date.now() - timerStart));
        console.log("--- END --- ok");
        reply({
          locations: output
        });
      }, function(error) {
        console.log("TIME total: " + (Date.now() - timerStart));
        console.log("--- END --- error" + JSON.stringify(error));
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
    return dbh.getLocationsForUserQuery(userId, timeStart, timeEnd).bind(this).then(function(results) {
      var locations = results.hits.hits;
      locations = _.map(locations, function(location) {
        return this.getLocationFromDbModel(location);
      }.bind(this));
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
    
    var timerStart = Date.now();
    return dbh.fetchPointer(USERID_TO_LOCID, currentUserId).bind(this).then(function(latestLocation) {
        console.log("TIME fetch latest: " + (Date.now() - timerStart));
        console.log("before compression: " + locations.length);
        locations = this.compressLocations(locations, latestLocation);
        console.log("after compression: " + locations.length);
        return Promise.resolve();
      })
      .then(function() {
        timerStart = Date.now();
        return this.getLocationsNearLocations(locations, currentUserId);
        // return Promise.resolve([]);
      })
      .then(function(locationsNearLocations) {
        console.log("TIME locations nearby: " + (Date.now() - timerStart));
        console.log("near loc: " + locationsNearLocations.length);
        return [locationsNearLocations, this.saveLocations(locations, currentUserId)];
      })
      .get(0);
  },

  saveLocations: function(locations, userId) {
    var timerStart = Date.now();
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    // Save locations after getLocationsNearLocations, because we need the 'processed' flag set
    var saveLocationsPromise = dbh.saveListToDB(locations);

    // Now we already assigned ids to locations objects, even though the list is not saved yet
    // So we can safely get the id of the last location
    var lastLocationId = _.last(locations)._id;
    var savePointerToLastLocation = dbh.savePointerToDB(USERID_TO_LOCID, userId, lastLocationId, "locations", "location");
    return Promise.all(saveLocationsPromise, savePointerToLastLocation)
      .then(function(result) {
        console.log("TIME save: " + (Date.now() - timerStart));
        return Promise.resolve(result);
      });
  },

  /* If promise is fulfilled, add {processed: true} to location
  [
    {
      location: {...},
      nearbyLocations: [ {..}, {..}],
    }
    ,
    ...
  ]
  */
  getLocationsNearLocations: function(locations, currentUserId) {
    // console.log(JSON.stringify(locations));
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    var tasks = _.map(locations, function(location) {
      return this.getLocationsNearSingleLocation(location, currentUserId);
    }.bind(this));

    return Promise.settle(tasks).bind(this).then(function(results) {
      var locationsNearLocations = [];
      _.each(results, function(result) {
        if (result.isFulfilled()) {
          var pair = result.value();
          pair.location._source.processed = true;
          locationsNearLocations.push(pair);
        }
      });
      return Promise.resolve(locationsNearLocations);
    });
  },

  /*
    {
      location: {},
      nearbyLocations: []
    }
  */
  getLocationsNearSingleLocation: function(location, currentUserId) {
    var timerStart = Date.now();
    return dbh.getLocationsNearSingleLocation(location, currentUserId).then(function(nearbyLocations) {
      console.log("TIME multiple: " + (Date.now() - timerStart));
      // Remove nearby locations that belong to current user
      console.log("nearby: " + JSON.stringify(nearbyLocations.hits.hits.length));
      // TODO Move this filter before fetching objects
      // nearbyLocations = this.filterLocationsFromCurrentUserId(nearbyLocations, location._source.userId);
      // console.log("nearby after: " + nearbyLocations.length);
      var object = {
        location: location,
        nearbyLocations: nearbyLocations.hits.hits
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
        _index: "locations",
        _type: "location",
        _id: uuid.v1(),
        _source: {
          location: {
            lat: location["latitude"],
            lon: location["longitude"]
          },
          timeStart:      location["time"],
          timeEnd:        location["time"],
          timeSpent:      0,
          accuracy:       location["accuracy"],
          userId:         userId
        }
      };
    });
  },
  getLocationFromDbModel: function(location) {
    return _.extend({}, location._source, {
      latitude: location._source.location.lat,
      longitude: location._source.location.lon,
      objectId: location._id
    });
  },

  compressLocations: function(locations, latestLocation) {
    console.log("locs: " + JSON.stringify(locations.length));
    console.log("last: " + JSON.stringify(latestLocation._id));
    var compressedLocations = [];

    if (locations.length === 0) {
      return compressedLocations;
    }
    if (latestLocation._source) {
      locations = this.filterOlderLocations(locations, latestLocation._source.timeStart);
      if (locations.length === 0) {
        return compressedLocations;
      }
      // If previous location ends after the new location start, it means that the previous location had a bigger
      // expiration than needed. In that case, we change previous location timeEnd so that it won't overlap with
      // the new location.
      var newLocation = _.first(locations);
      if (newLocation._source.timeStart < latestLocation._source.timeEnd) {
        latestLocation._source.timeEnd = newLocation._source.timeStart;
        latestLocation._source.timeSpent = latestLocation._source.timeEnd - latestLocation._source.timeStart;
      }
    } else {
      latestLocation = _.first(locations);
    }
    compressedLocations.push(latestLocation);

    _.each(locations, function(newLocation) {
      var newLocationPoint = {latitude: newLocation._source.location.lat, longitude: newLocation._source.location.lon};
      var latestLocationPoint = {latitude: latestLocation._source.location.lat, longitude: latestLocation._source.location.lon};

      // If new location is almost the same as previous location => merge the new one into the previous one
      if (geolib.getDistance(newLocationPoint, latestLocationPoint) < LOCATION_DIFFERENCE) {
        latestLocation._source.timeEnd = newLocation._source.timeEnd;
        latestLocation._source.timeSpent = latestLocation._source.timeEnd - latestLocation._source.timeStart;
      } else {
        latestLocation = newLocation;
        compressedLocations.push(latestLocation);
      }
    });

    // It's safe to supose that the user will stay here for the next x hours
    // until he uploads a new locaiton. In that case we'll shrink that time interval.
    // Add 2 hours offset to the latest location.
    latestLocation._source.timeEnd = latestLocation._source.timeEnd + 2 * 3600000;
    latestLocation._source.timeSpent = latestLocation._source.timeSpent + 2 * 3600000;
    console.log("comp locs: " + compressedLocations.length);
    return compressedLocations;
  },

  filterOlderLocations: function(locations, olderThan) {
    // Filter out old locations
    return _.filter(locations, function(location) {
      var timeStart = location._source.timeStart;
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