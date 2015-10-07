var couchbase = require('couchbase');
var cluster = new couchbase.Cluster('127.0.0.1');
var bucket = cluster.openBucket('location');
var ViewQuery = couchbase.ViewQuery;

var Promise = require("bluebird");
var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var geolib = require('geolib');

var LOCATION_DIFFERENCE = 50;
// 10 minutes overlap to consider that two locations intersect in time
var TIME_OFFSET = 10 * 60000;
// The length of the bounding box half edge(radius) in degrees
// http://msi.nga.mil/MSISiteContent/StaticFiles/Calculators/degree.html
// 1 degree = 110575m
var BBOX_EDGE = 0.001356545;  // 150m

var USERID_TO_LOCID = "userid_to_locid";
var SEPARATOR = "::";

var Locations = {
  handleLocationsRequest: function(request, reply) {
    var payload = JSON.parse(request.payload);
    var currentUserId = payload.user_id;
    var locations = payload.locations;

    this.processLocations(locations, currentUserId).bind(this).then(function(result) {
        reply(result);
      }, function(error) {
        reply(error);
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
    locations = this.filterBadLocations(locations);
    locations = this.sortLocations(locations);
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
      })
      .then(function(locationsNearLocations) {
        return [locationsNearLocations, this.saveLocations(locations, currentUserId)];
      })
      .get(0);
  },

  saveLocations: function(locations, userId) {
    if (locations.length === 0) {
      return Promise.resolve([]);
    }
    // Save locations after getLocationsNearLocations, because we need the 'processed' flag set
    var saveLocationsPromise = this.saveListToDB(locations, "Location");
    // Now we already assigned ids to locations objects, even though the list is not saved yet
    // So we can safely get the id of the last location
    var lastLocationId = _.last(locations)["object_id"];
    var savePointerToLastLocation = this.savePointerToDB(USERID_TO_LOCID + SEPARATOR + userId, lastLocationId);
    return Promise.all(saveLocationsPromise, savePointerToLastLocation);
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

  /* If promise is fulfilled, add {processed: true} to location
    {
      location: {},
      nearbyLocations: []
    }
  */
  getLocationsNearSingleLocation: function(location) {
    var nearLocationQuery = ViewQuery.fromSpatial("spatial", "location_space_time");

    var startRange = [location.latitude - BBOX_EDGE,
                      location.longitude - BBOX_EDGE,
                      location.time_start - TIME_OFFSET];
    var endRange = [location.latitude + BBOX_EDGE,
                    location.longitude + BBOX_EDGE,
                    location.time_start + TIME_OFFSET];
    nearLocationQuery.custom({
      start_range: JSON.stringify(startRange),
      end_range: JSON.stringify(endRange),
    });

    return new Promise(function(resolve, reject) {
      bucket.query(nearLocationQuery, function(error, data, meta) {
        var locationIds = _.map(data, function(location) {
          return location.id;
        });
        var object = {location: location};
        if (locationIds.length > 0) {
          this.fetchMultiObjects(locationIds).bind(this).then(function(results) {
            object.nearbyLocations = results;
            resolve(object);
          });
        } else {
          object.nearbyLocations = [];
          resolve(object);
        }
      }.bind(this));
    }.bind(this));
  },

  mapLocationsToDBModel: function(locations, userId) {
    return _.map(locations, function(location) {
      return {
        latitude:       location["latitude"],
        longitude:      location["longitude"],
        time_start:     location["time"],
        time_end:       location["time"],
        time_spent:     0,
        user_id:        userId,
      };
    });
  },

  saveListToDB: function(objects, className) {
    var saveTasks = _.map(objects, function(object) {
      return this.saveObjectToDB(object, className);
    }.bind(this));
    return Promise.settle(saveTasks);
  },
  saveObjectToDB: function(object, className) {
    this.assignDBModelInfo(object, className);
    return new Promise(function(resolve, reject) {
      bucket.upsert(object["object_id"], object, {}, function(error, result) {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  },
  savePointerToDB: function(key, toId) {
    var object = {
      object_id: key,
      to_id: toId
    };
    return this.saveObjectToDB(object, "Pointer");
  },
  assignDBModelInfo: function(object, className) {
    var key = object["object_id"] || uuid.v1();
    object["object_id"] = key;
    object["doc_type"] = className;
  },

  fetchMultiObjects: function(ids) {
    return new Promise(function(resolve, reject) {
      bucket.getMulti(ids, function(error, results) {
        results = results || [];
        // get array of values
        var objects = _.map(results, function(result) {
          return result.value;
        });
        // filter out null objects(couldn't be retrieved)
        objects = _.filter(objects, function(object) {
          return !!object;
        });
        resolve(objects);
      });
    });
  },
  fetchObject: function(id) {
    return new Promise(function(resolve, reject) {
      bucket.get(id, function(error, result) {
        var object = result && result.value;
        resolve(object);
      });
    });
  },
  fetchPointer: function(id) {
    return this.fetchObject(id).bind(this).then(function(result) {
      if (result) {
        return this.fetchObject(result["to_id"] || "");
      } else {
        return Promise.resolve(null);
      }
    })
  },


  compressLocations: function(locations, latestLocation) {
    var compressedLocations = [];

    if (locations.length === 0) {
      return compressedLocations;
    }
    if (latestLocation) {
      locations = this.filterOlderLocations(locations, latestLocation["time_end"]);
      if (locations.length === 0) {
        return compressedLocations;
      }
      // If previous location ends after the new location start, it means that the previous location had a bigger
      // expiration than needed. In that case, we change previous location time_end so that it won't overlap with
      // the new location.
      var newLocation = _.first(locations);
      if (latestLocation["time_end"] > newLocation["time_start"]) {
        latestLocation["time_end"] = newLocation["time_start"];
        latestLocation["time_spent"] = latestLocation["time_end"] - latestLocation["time_start"];
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
        latestLocation["time_end"] = newLocation["time_end"];
        latestLocation["time_spent"] = latestLocation["time_end"] - latestLocation["time_start"];
      } else {
        latestLocation = newLocation;
        compressedLocations.push(latestLocation);
      }
    });

    // It's safe to supose that the user will stay here for the next x hours
    // until he uploads a new locaiton. In that case we'll shrink that time interval.
    // Add 2 hours offset to the latest location.
    latestLocation["time_end"] = latestLocation["time_end"] + 2 * 3600000;
    latestLocation["time_spent"] = latestLocation["time_spent"] + 2 * 3600000;

    return compressedLocations;
  },

  fetchLatestLocation: function(userId) {
    return this.fetchPointer(USERID_TO_LOCID + SEPARATOR + userId);
  },

  filterOlderLocations: function(locations, olderThan) {
    assert(olderThan === parseInt(olderThan, 10), "olderThan: expected integer");
    // Filter out old locations
    return _.filter(locations, function(location) {
      var timeEnd = location["time_end"];

      assert(timeEnd === parseInt(timeEnd, 10), "timeEnd: expected integer");

      return !(timeEnd < olderThan);
    });
  },

  filterBadLocations: function(locations) {
    // Filter out bad locations
    return _.filter(locations, function(location) {
      var latitude = location["latitude"];
      var longitude = location["longitude"];
      return -90 < latitude && latitude < 90 && -90 < longitude && longitude < 90;
    });
  },
  sortLocations: function(locations) {
    // Sort locations by time
    return _.sortBy(locations, function(location) {
      return location["time"];
    });
  },

  saveLocationsToMongo: function(locations) {
    var newLoc = locations[0];
    var db = request.server.plugins['hapi-mongodb'].db;
    db.collection('locations').insert(newLoc, { w: 1 }, function (err, doc){
      if (err){
        return reply(Hapi.error.internal('Internal MongoDB error', err));
      } else {
        reply(doc);
      }
    });
  },

};

module.exports = Locations;