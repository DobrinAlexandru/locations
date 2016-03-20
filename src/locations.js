var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var geolib = require('geolib');
var uuid = require('node-uuid');
var utils = require('./utils');

var LOCATION_DIFFERENCE = 75;
var USERID_TO_LOCID = "useridToLocid";

var Locations = {
  handleLocationsRequest: function(payload) {
    return this.processLocations(payload.locations, payload.userId, payload.radius).bind(this)
      .then(function(locations) {
        return Promise.resolve({
          locations: locations
        });
      });
  },

  getLocationsForUser: function(payload) {
    // Verify pw
    if (payload.pw !== "4loc4") {
      return Promise.resolve({locations: []});
    }
    return this.getLocationsForUserBetweenDates(payload.userId, payload.timeStart, payload.timeEnd).bind(this)
      .then(function(locations) {
        return Promise.resolve({
          locations: locations
        });
      });
  },

  getLatestLocationsByUser: function(payload) {
    // Verify pw
    if (payload.pw !== "4loc4") {
      return Promise.resolve({locations: []});
    }
    if (payload.nrUsers * payload.nrLocations > 30000) {
      return Promise.resolve({locations: ["Fuckere, cere mai putine"]});
    }
    return dbh.getLatestLocationsByUser(payload.nrUsers, payload.nrLocations, payload.timeStart, payload.timeEnd).bind(this)
      .then(function(result) {
        return Promise.resolve({
          locations: result.aggregations.latestByUser.buckets
        })
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
  processLocations: function(locations, currentUserId, radius) {
    locations = this.filterAndFixBadLocations(locations);
    locations = _.sortBy(locations, "time");
    locations = this.mapLocationsToDBModel(locations, currentUserId);
    
    var timerStart = Date.now();
    return dbh.fetchPointer(USERID_TO_LOCID, currentUserId).bind(this).then(function(latestLocation) {
        // console.log("TIME fetch latest: " + (Date.now() - timerStart));
        // console.log("before compression: " + locations.length);
        locations = this.compressLocations(locations, latestLocation);
        // TODO remove this when server is more stable
        // locations = _.last(locations, 1);
        // console.log("after compression: " + locations.length);
        return Promise.resolve([]);
      })
      .then(function() {
        timerStart = Date.now();
        return this.getLocationsNearLocations(_.last(locations, 10), currentUserId, radius);
        // return Promise.resolve([]);
      })
      .then(function(locationsNearLocations) {
        // console.log("TIME locations nearby: " + (Date.now() - timerStart));
        console.log("near loc: " + locationsNearLocations.length);
        // Save locations after getLocationsNearLocations, because we need the 'processed' flag set
        return Promise.all([Promise.resolve(locationsNearLocations), this.saveLocations(locations, currentUserId)]);
      })
      // .then(function(results) {
      //   return Promise.reject();
      // })
      .get(0);
  },

  saveLocations: function(locations, userId) {
    var timerStart = Date.now();
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    // If not time machine
    if (!(locations.length === 1 && locations[0].timeMachine)) {
      // We assign CUSTOM ID only to the LAST LOCATION. We let elasticsearch to provide ids to the others
      // We need this in order to do locations saving and pointer saving in paralel.
      var lastLocation = _.last(locations);
      if (!lastLocation._id) lastLocation._id = uuid.v1();
      // Create pointer to last user location
      var pointerToLastUserLocation = dbh.createPointerObject(USERID_TO_LOCID, userId, lastLocation._id, "locations", "location");
      // Attach to list of objects and save in bulk
      locations.push(pointerToLastUserLocation);
    }
    return dbh.saveListToDB(locations).then(function(result) {
        console.log("TIME save: " + (Date.now() - timerStart));
        return Promise.resolve();
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
  getLocationsNearLocations: function(locations, currentUserId, radius) {
    // console.log(JSON.stringify(locations));
    if (locations.length === 0) {
      return Promise.resolve([]);
    }

    var tasks = _.map(locations, function(location) {
      return this.getLocationsNearSingleLocation(location, currentUserId, radius).reflect();
    }.bind(this));

    return Promise.all(tasks).bind(this).then(function(results) {
      var locationsNearLocations = [];
      _.each(results, function(result) {
        if (result.isFulfilled()) {
          var pair = result.value();
          pair.location._source.processed = true;
          // Push only the locations that have locations nearby
          // if (pair.nearbyLocations.length > 0) {
          locationsNearLocations.push(pair);
          // }
        }
      });
      // console.log("near loc: " + JSON.stringify(locationsNearLocations));
      return Promise.resolve(locationsNearLocations);
    });
  },

  /*
    {
      location: {},
      nearbyLocations: []
    }
  */
  getLocationsNearSingleLocation: function(location, currentUserId, radius) {
    var timerStart = Date.now();
    return dbh.getLocationsNearSingleLocation(location, currentUserId, radius).then(function(nearbyLocations) {
      console.log("TIME multiple: " + (Date.now() - timerStart));
      console.log("nearby: " + nearbyLocations.hits.hits.length);
      var object = {
        location: location,
        nearbyLocations: nearbyLocations.hits.hits
      };
      return Promise.resolve(object);
    });
  },

  mapLocationsToDBModel: function(locations, userId) {
    return _.map(locations, function(location) {
      var c = location.timeMachine ? parseInt(utils.C.HOUR) : 0;
      return {
        _index: "locations",
        _type: "location",
        // _id: uuid.v1(),
        timeMachine: location["timeMachine"],
        _source: {
          location: {
            lat: location["latitude"],
            lon: location["longitude"]
          },
          timeStart:      location["time"] - c,
          timeEnd:        location["time"] + c,
          timeSpent:      c * 2,
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
    console.log("last: " + latestLocation._id);
    var compressedLocations = [];

    if (locations.length === 0) {
      return compressedLocations;
    }
    // if time machine
    if (locations.length === 1 && locations[0].timeMachine) {
      return locations;
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
        latestLocation._source.timeSpent += parseInt(utils.C.HOUR / 2);
        latestLocation._source.timeEnd += parseInt(utils.C.HOUR / 2);

        // If previous location ends after the new location start, it means that the previous location had a bigger
        // expiration than needed. In that case, we change previous location timeEnd so that it won't overlap with
        // the new location.
        if (newLocation._source.timeStart < latestLocation._source.timeEnd) {
          // Combine old timeEnd with newTimeStart, but leave at least a 5 minutes gap
          latestLocation._source.timeEnd = Math.max(newLocation._source.timeStart - parseInt(utils.C.HOUR / 12), latestLocation._source.timeStart);
          latestLocation._source.timeSpent = latestLocation._source.timeEnd - latestLocation._source.timeStart;
        }

        latestLocation = newLocation;
        compressedLocations.push(latestLocation);
      }
    });

    // It's safe to supose that the user will stay here for the next x hours
    // until he uploads a new location. In that case we'll shrink that time interval.
    // Add 2 hours offset to the latest location.
    latestLocation._source.timeEnd = latestLocation._source.timeEnd + 2 * utils.C.HOUR;
    latestLocation._source.timeSpent = latestLocation._source.timeSpent + 2 * utils.C.HOUR;
    return compressedLocations;
  },

  filterOlderLocations: function(locations, olderThan) {
    // Filter out old locations
    return _.filter(locations, function(location) {
      var timeStart = location._source.timeStart;
      return !(timeStart < olderThan) || location.timeMachine;
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
      } else if (location["time"] > Date.now() + utils.C.HOUR) {
        // If locations is more than 1 hour into the future, set current time
        location["time"] = Date.now();
      }
      return -90 < latitude && latitude < 90 && -90 < longitude && longitude < 90;
    });
  },
};

module.exports = Locations;