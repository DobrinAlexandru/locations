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
    var timerStart = Date.now();
    return this.processLocations(payload.locations, payload.userId, payload.radius).bind(this)
      .then(function(locations) {
      //var lastlocation  = _.last(locations);
      //console.log("lasssst locations" + JSON.stringify(lastlocation));
     console.log("time till save locations" + (Date.now() - timerStart));
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
    if (payload.userId === "EIxcvQA5J6" && (((new Date(parseInt(payload.timeStart))).getMinutes()) !== 11)) {
      return Promise.resolve({locations: ["Gustere..."]});
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
    var timerStart = Date.now();
    //console.log("before filter" + locations.length);
    locations = this.filterAndFixBadLocations(locations);
    //console.log("time0.1 :" +(Date.now() - timerStart));
    //console.log("after filter" + locations.length);
    locations = _.sortBy(locations, "time");
    //console.log("time0.2 :" +(Date.now() - timerStart));
    locations = this.mapLocationsToDBModel(locations, currentUserId);
    locations = _.last(locations, 100);
    // if (Math.random() > 1) {
    //   return Promise.reject();
    //   return this.saveLocations(locations, currentUserId);
    // }
  
    //console.log("time0.3 :" +(Date.now() - timerStart));
    return dbh.getLastLocationToRedis(currentUserId).bind(this).then(function(latestLocation) {
        // console.log("TIME fetch latest: " + (Date.now() - timerStart));
        latestLocation = latestLocation.body;
        //console.log("before compression: " + locations.length);
        console.log("time1 :" +(Date.now() - timerStart));
        //console.log("latestlocations" + JSON.stringify(latestLocation));
        locatios = this.compressLocations(locations, latestLocation);
        // TODO remove this when server is more stable
        // locations = _.last(locations, 1);
         //console.log("after compression: " + locations.length);
        //console.log("time2 :" +(Date.now() - timerStart));
        return Promise.resolve([]);
      })
      .then(function() {
       // console.log("time3 :" +(Date.now() - timerStart));
        //console.log("locations to fetch" + JSON.stringify(locations))
        return this.getLocationsNearLocations(_.last(locations, 30), currentUserId, radius);
        // return Promise.resolve([]);
      })
      .then(function(locationsNearLocations) {
        // console.log("TIME locations nearby: " + (Date.now() - timerStart));
        //console.log("near loc: " + JSON.stringify(locationsNearLocations));
        console.log("time4 :" +(Date.now() - timerStart));
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
    return Promise.all([dbh.saveListToDB(locations), dbh.saveListToRedis(locations)]).then(function(){
        console.log("time to save" + (Date.now() - timerStart));
        return Promise.resolve({});
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
  getLocationsNearSingleLocation: function(location, currentUserId, radius) {
    var timerStart = Date.now();
   console.log("get near location");
    if(timerStart - location._source.timeStart < utils.C.DAY ) {
      console.log("redis search" + location._source.timeStart);
	    return dbh.getLocationsNearSingleLocationFromRedis(location, currentUserId, radius).then(function(nearbyLocations) {
	     // console.log("redis location found" + JSON.stringify(nearbyLocations.body));
	      console.log("TIME multiple: " + (Date.now() - timerStart));
	      console.log("nearby: " + nearbyLocations.body.hits.hits.length);
	      var object = {
	        location: location,
	        nearbyLocations: nearbyLocations.body.hits.hits
	      };
	      return Promise.resolve(object);
	    });
	} else {
    console.log("esss");
		return dbh.getLocationsNearSingleLocation(location, currentUserId, radius).then(function(nearbyLocations) {
	      console.log("TIME multiple: " + (Date.now() - timerStart));
	      console.log("nearby: " + nearbyLocations.hits.hits.length);
	      var object = {
	        location: location,
	        nearbyLocations: nearbyLocations.hits.hits
	      };
	      return Promise.resolve(object);
	     });
	}
  },

  getLocationsNearLocations: function(locations, currentUserId, radius) {
    // console.log(JSON.stringify(locations));
    if (locations.length === 0) {
      return Promise.resolve([]);
    }


    var tasks = _.map(locations, function(location) {
      return this.getLocationsNearSingleLocation(location, currentUserId, radius).reflect();
    }.bind(this));
    console.log("aaa");
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

   createFakeLocationForFakeUsers: function(fakeUsers, latitude, longitude, time) {
     return _.map(fakeUsers, function(fakeUser) {
      var c = parseInt(utils.C.HOUR) ;
       return {
          _index: "locations",
          _type: "location",
           //_id: uuid.v1(),
          timeMachine: true,
          _source: {
            location: {
              lat: latitude,
              lon: longitude
            },
            timeStart:      time - c,
            timeEnd:       time + c,
            timeSpent:      c * 2,
            accuracy:      50,
            userId:         fakeUser._id
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
  
 getUserForTimeMachine: function(location, currentUserId, radius, gender, interestedInMin, interestedInMax, age,  tryFakeUsers, genderInt) {
    var timerStart = Date.now();

    return this.getLocationsNearLocations(location, currentUserId, radius).then(function(nearbyLocations){
         var otherUsersIds = [];
         otherUsersIds.push(currentUserId);
         var locations  = nearbyLocations[0].nearbyLocations;
         _.each(nearbyLocations[0].nearbyLocations, function(location) {
            otherUsersIds.push(location._source.userId);
          });
          return Promise.resolve(dbh.fetchMultiObjects(otherUsersIds, "users", "user"));
        }).then(function(usersFetched){
          usersFetched = usersFetched.docs;
         //filter user
          var filteredUsers  = [];
          console.log("enteredUsers" + usersFetched.length);
          var userAge = age;
          // If people didn't change the 6 years interval, add 4 more years to the interval.
          var ageOffset =  2 ;var index = 0; var mainUser;
          _.each(usersFetched, function(user) {
            if(index == 0){
              mainUser = user; mainUser._source.gender = gender; mainUser._source.genderInt =  genderInt; index ++;
            } else {
              if(user != null ){  
                if(utils.genderFilterMatch(user, mainUser)){
                  if (user._source.birthday) {
                    if(user._source.ageIntMax >  userAge - ageOffset && user._source.ageIntMin < userAge + ageOffset 
                      && user._source.birthday > utils.birthday(user._source.ageIntMax + ageOffset) && user._source.birthday < utils.birthday(user._source.ageIntMin - ageOffset)) {
                       filteredUsers.push(user._source.fbid);
                    }
                 } else {
                     filteredUsers.push(user._source.fbid);
                 }
                } 
              }
            }
          });
          var object = {
             numberOfUsers: filteredUsers.length,
             usersFbidList: filteredUsers
          };

          if(filteredUsers.length === 0 && tryFakeUsers){
            mainUser._source.ageIntMin = interestedInMin;
            mainUser._source.ageIntMax = interestedInMax;
            return Promise.all([filteredUsers, dbh.pickAvailableFakeUsers(mainUser, 5, genderInt, gender)]);
         } else {
         return Promise.all([object, []]);
         }
       }).spread(function(filteredUsers, fakeUsers){
          if(fakeUsers.length != 0){
            fakeUsers = fakeUsers.hits.hits;
            var object = {};

            if(fakeUsers != null && fakeUsers.length > 0 ) {
                // update lastTimeFake for used fake users
                var currentTime = Date.now();

                var fakeNearbyLocations = this.createFakeLocationForFakeUsers(fakeUsers, location[0]._source.location.lat, location[0]._source.location.lon, location[0]._source.timeStart);
                
                var fakeUsersUpdates = _.map(fakeUsers, function(fakeUser) {
                  var update = _.pick(fakeUser, "_index", "_type", "_id");
                  update.doc = {
                    lastTimeFake: currentTime
                  };
                  return update;
                });
                var filteredUsers2 = utils.getFbIdListFromListOfUsers(fakeUsers);
                 object = {
                 numberOfUsers: filteredUsers2.length,
                 usersFbidList: filteredUsers2
                };
            } else {
                object = {
                 numberOfUsers: 0,
                 usersFbidList: []
                };
            }
            return Promise.all([object, dbh.updateListToDB(fakeUsersUpdates), dbh.saveListToDB(fakeNearbyLocations)]);
          } else {
            return Promise.all([filteredUsers,[],[]]);
          }
          
      }).spread(function(filteredUsers, updateState1, updateState2){
          return Promise.resolve(filteredUsers);
      });
  },

  compressLocations: function(locations, latestLocation) {
    console.log("last: " + latestLocation._id);
    var compressedLocations = [];

    if (locations.length === 0 || latestLocation == "undefined") {
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