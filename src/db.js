var _ = require('underscore');
var Promise = require("bluebird");
var uuid = require('node-uuid');
var requestLib = Promise.promisify(require("request"));
var utils = require('./utils');

var SEPARATOR = "::";
// The length of the bounding box half edge(radius) in degrees
// http://msi.nga.mil/MSISiteContent/StaticFiles/Calculators/degree.html
// 1 degree = 110575m
var BBOX_EDGE = [0.001356545, 0.002713090, 0.004521817]  // 150m, 300m, 500m
// 10 minutes overlap to consider that two locations intersect in time
var TIME_OFFSET = 20 * 60000;
// Time interval we search for should not expand more than 1day before & after location time
var TIME_BOUND = 24 * 3600000;

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'api.gointersect.com:9200',
  maxSockets:2500,
  // log: 'trace'
  log : [{
    type: 'stdio',
    levels: ['error', 'warning'] // change these options
  }]
});
console.log("xxxxx bog xxxxx");
 
var db = {
  saveLastMacObjectsToRedis : function(object){
   var postUrl = "http://es02.gointersect.com:8001/api/saveLastMacObjects";
      console.log("postUrl" + postUrl);
      return requestLib({
        url: postUrl,
        method: 'POST',
        json: true,
        body: {objects: objectsToSave, password:"4loc4"}
      });
  },
  saveLastLocationToRedis : function(object){
   var postUrl = "http://es02.gointersect.com:8001/api/saveLastLocation";
      console.log("postUrl" + postUrl);
      return requestLib({
        url: postUrl,
        method: 'POST',
        json: true,
        body: {objects: objectsToSave, password:"4loc4"}
      });
  },
  getLastLocationToRedis : function(object){
   var postUrl = "http://es02.gointersect.com:8001/api/getLatestLocation";
      console.log("postUrl" + postUrl);
      return requestLib({
        url: postUrl,
        method: 'POST',
        json: true,
        body: {currentUserId: object, password:"4loc4"}
      });
  },
   getLastMacAddressesFromRedis : function(object){
   var postUrl = "http://es02.gointersect.com:8001/api/getLastMacObjects";
      console.log("postUrl" + postUrl);
      return requestLib({
        url: postUrl,
        method: 'POST',
        json: true,
        body: {currentUserId: object, password:"4loc4"}
      });
  },

  saveListToRedis: function(objects){
      //post objects to redis
     // console.log("save to redis" + JSON.stringify(objects));
      var objectsToSave = [];
      var currentTime = Date.now();
      var type = "";
      _.each(objects, function(object){
          if(currentTime - object._source.timeStart < utils.C.DAY ){
            
            if(object._type=="macobject"){
              console.log("type" + JSON.stringify(object._type));
              type = "macobject";
            } else {
              type = "locations";
            } 
            objectsToSave.push(object);  
          } else {
            //console.log("old" + (currentTime - object._source.timeStart));
          }
      });


      if(type == "macobject"){
        type = "saveMacObjects";
      } else {
        type = "saveLocations";
      }

      if(objectsToSave != null && objectsToSave.length > 0){
        console.log("enter save redis");
        var postUrl = "http://es02.gointersect.com:8001/api/" + type;
        console.log("postUrl" + postUrl);
        return requestLib({
          url: postUrl,
          method: 'POST',
          json: true,
          body: {objects: objectsToSave, password:"4loc4"}
        });
      } else {
          console.log("dont save to redis");
          return objectsToSave;
      }
  },

  saveMacObjectsToRedis: function(objects, lastMacIds){
      //post objects to redis
     // console.log("save to redis" + JSON.stringify(objects));
      var objectsToSave = [];
      var currentTime = Date.now();
      _.each(objects, function(object){
          if(currentTime - object._source.timeStart < utils.C.DAY ){
            objectsToSave.push(object);  
          } 
      });

      if(objectsToSave != null && objectsToSave.length > 0){
        console.log("enter save redis");
        var postUrl = "http://es02.gointersect.com:8001/api/saveMacObjects";
        console.log("postUrl" + postUrl);
        return requestLib({
          url: postUrl,
          method: 'POST',
          json: true,
          body: {objects: objectsToSave, password:"4loc4", lastMacAddressIds :lastMacIds}
        });
      } else {
          console.log("dont save to redis");
          return objectsToSave;
      }
  },

  saveListToDB: function(objects) {
    var bulkOperations = [];
    _.each(objects, function(object) {
      var operation = {
        index: _.pick(object, "_index", "_type", "_id")
      }
      bulkOperations.push(operation, object._source);
    });

    return Promise.resolve(client.bulk({
      body: bulkOperations
    }));
  },
  saveObjectToDB: function(object) {
    return Promise.resolve(client.index({
      index: object._index,
      type: object._type,
      id: object._id,
      body: object._source
    })).then(function(result) {
      if (!object._id) {
        object._id = result._id;
      }
      return Promise.resolve(object);
    });
  },
  updateListToDB: function(objects) {
    var bulkOperations = [];
    _.each(objects, function(object) {
      var operation = {
        update: _.pick(object, "_index", "_type", "_id")
      }
      bulkOperations.push(operation, _.pick(object, "doc", "upsert"));
    });

    return Promise.resolve(client.bulk({
      body: bulkOperations
    }));
  },
  updateObjectToDb: function(object) {
    return Promise.resolve(client.update({
      index: object._index,
      type: object._type,
      id: object._id,
      body: _.pick(object, "doc", "upsert")
    }));
  },
  increment: function(object, field, amount) {
    // Increment object in memory
    object._source[field] = (object._source[field] || 0) + 1;
    // Increment object in db
    return this.incrementToDb(object._id, object._index, object._type, field, amount);
  },
  incrementToDb: function(id, index, type, field, amount) {
    return Promise.resolve(client.update({
      index: index,
      type: type,
      id: id,
      body: {
        script: {
          "id": "increment",
          "params": {
            "field": field,
            "amount": amount
          }
        }
      }
    }));
  },

  createPointerObject: function(pointerType, fromId, id, index, type) {
    return {
      _index: "pointers",
      _type: pointerType,
      _id: fromId,
      _source: {
        index: index,
        type: type,
        id: id
      }
    };
  },

  savePointerToDB: function(pointerType, fromId, id, index, type) {
    return this.saveObjectToDB(this.createPointerObject(pointerType, fromId, id, index, type));
  },

  fetchMultiObjects: function(ids, index, type) {
    return this.fetchMulti({
      index: index,
      type: type,
      body: {
        ids: ids
      }
    }, ids.length);
  },
  
  fetchMulti: function(params, size) {
    params = _.extend(params, {
      ignore: [404],
      size: size || 100,
    });
    return Promise.resolve(client.mget(params));
  },
  fetchObject: function(id, index, type) {
    return this.fetch({
      index: index,
      type: type,
      id: id
    });
  },
  fetch: function(params) {
    params = _.extend(params, {
      ignore: [404],
    });
    return Promise.resolve(client.get(params));
  },
  fetchPointer: function(pointerType, fromId) {
    return this.fetchObject(fromId, "pointers", pointerType).bind(this).then(function(pointer) {
      return pointer._source ? this.fetch(pointer._source) : Promise.resolve({});
    }, function(error) {
      return Promise.reject(error);
    });
  },
  getLocationsForUserQuery: function(userId, timeStart, timeEnd, size) {
    return Promise.resolve(client.search({
      index: "locations",
      type: "location",
      size: size || 100,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                      "must": [
                          {"range": {
                              "timeStart": {
                                  "gt": timeStart,
                                  "lt": timeEnd
                              }
                          }},
                          {"term": {
                              "userId": userId
                          }}
                      ]
                  }
              }
          }
        }
      }
    }));
  },
  getLocationsNearSingleLocationFromRedis: function(location, excludeUserId, radius, size) {
    console.log("enter redis get near location");
    return requestLib({
        url: 'http://es02.gointersect.com:8001/api/processLocations',
        method: 'POST',
        json: true,
        body: {locations: location,  password:"4loc4",
        curretUserId : excludeUserId,
              }
      });
  },
  getLocationsNearSingleLocation: function(location, excludeUserId, radius, size) {
    radius = radius || 0;
    return Promise.resolve(client.search({
      index: "locations",
      type: "location",
      size: size || 100,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                      "must": [
                          {"geo_bounding_box": {
                            "type":    "indexed",
                            "location": { 
                                  "top_left": {
                                    "lat": location._source.location.lat + BBOX_EDGE[radius],
                                    "lon": location._source.location.lon - BBOX_EDGE[radius]
                                  },
                                  "bottom_right": {
                                    "lat":  location._source.location.lat - BBOX_EDGE[radius],
                                    "lon": location._source.location.lon + BBOX_EDGE[radius]
                                  }
                            }
                          }},
                          {"range": {
                              "timeEnd": {
                                  "gt":  location._source.timeStart - TIME_OFFSET,
                                  "lt":   Math.min(Date.now() + TIME_BOUND, location._source.timeStart + TIME_BOUND)
                              }
                          }},
                          {"range": {
                              "timeStart": {
                                  "gt":  location._source.timeEnd - TIME_BOUND,
                                  "lt":   location._source.timeEnd + TIME_OFFSET
                              }
                          }}
                      ],
                      "must_not": {
                          "term": {
                              "userId": excludeUserId
                          }
                      }
                  }
              }
          }
        }
      }
    }));
  },
  getLatestLocationsByUser: function(nrUsers, nrLocations, timeStart, timeEnd) {
    timeStart = timeStart || Date.now() - utils.C.HOUR;
    timeEnd = timeEnd || Date.now() + utils.C.DAY / 2;
    return Promise.resolve(client.search({
      index: "locations",
      type: "location",
      body: {
        "size": 0,
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                      "must": [
                          {"range": {
                              "timeEnd": {
                                  "gt": timeStart,
                                  "lt": timeEnd
                              }
                          }}
                      ]
                  }
              }
          }
        },
        "aggs": {
          "latestByUser": {
            "terms": {
              "field": "userId",
              "size": nrUsers,
              "order": {
                "sortingAgg": "desc"
              }
            },
            "aggs": {
               "latestLocations": {
                          "top_hits": {
                              "sort": [
                                  {
                                      "timeEnd": {
                                          "order": "desc"
                                      }
                                  }
                              ],
                              "size" : nrLocations
                          }
                      },
              "sortingAgg": {
                "max": {
                  "field": "timeEnd"
                }
              }
            }
          }
        }
      }
    }));
  },

  loadBumps: function(userData, reverse, skip, size) {
    var userId = userData.userId || userData.user._id;
    var otherUsersIds = userData.otherUsersIds;
    var user = userData.user;
    var friendStatus = userData.friendStatus;
    var applyFilters = userData.filters;
    var applySort = userData.sort;
    var retrieveHidden = userData.hidden;
    var seen = userData.seen;
    
    var term = {};
    term["user" + (!reverse ? "1" : "2") + ".userId"] = userId;
    var must = [
      {"term":  term}
    ];
    if (friendStatus) {
      must.push({"term": {"friendStatus": friendStatus}});
    }
    if (!(seen === undefined || seen === null)) {
      must.push({"term": {"seen": seen}});
    }
    var sort;
    if (applySort) {
      // Sort newsfeed
      sort = [
        { "updatedAt":   { "order": "desc" }}
      ];
    }

    if (otherUsersIds) {
      var terms = {};
      terms["user" + (reverse ? "1" : "2") + ".userId"] = otherUsersIds
      must.push({"terms": terms});
    }

    if (applyFilters) {
      // Age filter
      // My age is in others interests
      if (user._source.birthday) {
        must.push({
          "range": {
              "user2.ageIntMax": {
                  "gte": utils.age(user._source.birthday) - 2
              }
          }
        });
        must.push({
          "range": {
              "user2.ageIntMin": {
                  "lte": utils.age(user._source.birthday) + 2
              }
          }
        });
        // Others fall in my interests
        must.push({
          "range": {
              "user2.birthday": {
                  "gte": utils.birthday(user._source.ageIntMax + 2),
                  "lte": utils.birthday(user._source.ageIntMin - 2)
              }
          }
        });
      }

      // Gender filter
      // (x, y) <=> (y, x) || (y, 3)
      // (x, 3) <=> (-, x) || (-, 3)
      // (3, x) <=> (x, 3)
      // (3, 3) <=> (-, 3)

      if (user._source.gender === 3 && user._source.genderInt === 3) {
        must.push({
          "term": {"user2.genderInt": 3}
        });
      } else if (user._source.gender === 3) {
        must.push({
          "term": {"user2.gender": user._source.genderInt}
        });
        must.push({
          "term": {"user2.genderInt": 3}
        });
      } else if (user._source.genderInt === 3) {
        must.push({
          "bool" : {
            "should": [
              {"term": {"user2.genderInt": 3}},
              {"term": {"user2.genderInt": user._source.gender}}
            ]
          }
        });
      } else {
        must.push({
          "term": {"user2.gender": user._source.genderInt}
        });
        must.push({
          "bool" : {
            "should": [
              {"term": {"user2.genderInt": 3}},
              {"term": {"user2.genderInt": user._source.gender}}
            ]
          }
        });
      }
    }

    var bool = {
      "must": must
    };
    if (!retrieveHidden) {
      bool["must_not"] = {"term": {"hidden": true}};
    }

    return Promise.resolve(client.search({
      index: "bumps",
      type: "bump",
      size: size || 20,
      from: skip || 0,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": bool
              }
          }
        },
        "sort": sort
      }
    }));
  },

  pickAvailableFakeUsers: function(user, size, genderInt, gender) {
    var currentTime = Date.now();
    var must = [];
    if (gender === 3 && genderInt === 3) {
      console.log("a1");
        must.push({
          "term": {"genderInt": 3}
        });
      } else if (gender === 3) {
        console.log("a2");
        must.push({
          "term": {"gender": genderInt}
        });
        must.push({
          "term": {"genderInt": 3}
        });
      } else if (genderInt === 3) {
         console.log("a3");
        must.push({
          "bool" : {
            "should": [
              {"term": {"genderInt": 3}},
              {"term": {"genderInt": gender}}
            ]
          }
        });
      } else {
        must.push({
          "term": {"gender": genderInt}
        });
        must.push({
          "bool" : {
            "should": [
              {"term": {"genderInt": 3}},
              {"term": {"genderInt": gender}}
            ]
          }
        });
      }
      must.push({"range": {"birthday": {
                                  "gte": utils.birthday(user._source.ageIntMax),
                                  "lte": utils.birthday(user._source.ageIntMin)
                              }
                          }});
      must.push( {"range": {
                              "lastTimeFake": {
                                  "lt":   currentTime - utils.C.HOUR / 3
                              }
                          }});
      must.push( {"term": {
                              "isFake": true
                          }});

    var bool = {
      "must": must
    };
    return Promise.resolve(client.search({
      index: "users",
      type: "user",
      size: size || 100,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool":bool
              }
          }
        }
      }
    }));
  },
  loadConversations: function(userId, otherUsersIds, skip, size) {
    var must1 = [
      {"term":  {"user1.userId": userId}}
    ];
    var must2 = [
      {"term":  {"user2.userId": userId}}
    ];
    var sort;
    if (otherUsersIds) {
      must1.push({"terms": {"user2.userId": otherUsersIds}});
      must2.push({"terms": {"user1.userId": otherUsersIds}});
    } else {
      sort = [
        { "lastMsg.time":   { "order": "desc" }}
      ];
    }

    return Promise.resolve(client.search({
      index: "conversations",
      type: "conversation",
      size: size || 20,
      from: skip || 0,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                    "should": [
                      {"bool": {
                        "must": must1,
                        "must_not": {"term": {"user1.deleted": true}}
                      }},
                      {"bool": {
                        "must": must2,
                        "must_not": {"term": {"user2.deleted": true}}
                      }}
                    ]
                  }
              }
          }
        },
        "sort": sort
      }
    }));
  },

  fetchPointerList: function(pointerType, fromId) {
    console.log("fetch pointer list");
     return this.fetchObject(fromId, "pointers", pointerType).bind(this).then(function(pointer) {
      //console.log("\n\n\n\pointers list fetched: " + JSON.stringify(pointer));
      if(pointer._source != null){
        var newPointerIds = [];
         _.each(pointer._source.id, function(id){
            if(id != null){
              newPointerIds.push(id);
            }
         });
         pointer._source.id = newPointerIds;
      }

      return pointer._source ? this.fetchMultiObjects(pointer._source.id, "macobjects", "macobject") : Promise.resolve({});
    }, function(error) {
      console.log("error" + JSON.stringify(error))
      return Promise.reject(error);
    });
  },
  
  
  getMacAddressByAdress: function(excludeUserId, address, timeStart, timeEnd, size) {
    timeEnd = timeEnd || timeStart + utils.C.HOUR/2;
    return Promise.resolve(client.search({
      index: "macobjects",
      type: "macobject",
      size: size || 100,
      body: {
             "query" :{
                "filtered": {
                   "filter": {
                     "bool" : {
                      "must" : [
                           {"range": {
                              "timeEnd": {
                                  "gt":  timeStart - TIME_OFFSET,
                              }
                          }},
                          {"range": {
                              "timeStart": {
                                  "gt":  timeEnd - TIME_BOUND,
                                  "lt":  timeEnd + TIME_OFFSET
                              }
                          }},
                          {
                          "term": {
                              "address": address
                          }
                        }
                        ],
                        "must_not": {
                          "term": {
                              "userId": excludeUserId
                          }
                      }
                  }
                }
              }
            }
          }
    }));
  },


  getMacAddressByAdressFromRedis: function(excludeUserId, macobject, count) {
    console.log("enter redis get near macobject");
    return requestLib({
        url: 'http://es02.gointersect.com:8001/api/processMacObjects',
        method: 'POST',
        json: true,
        body: { macObjects: macobject,
                curretUserId : excludeUserId,
                size : count,
                 password:"4loc4"
              }
      });
  },

  getMacAddressByUser: function(userId, size, timeStart, timeEnd) {
    timeStart = timeStart || Date.now() - utils.C.HOUR;
    timeEnd = timeEnd || Date.now() + utils.C.DAY / 2;
     var sort = [
        { "timeStart":   { "order": "asc" }}
      ];
    return Promise.resolve(client.search({
      index: "macobjects",
      type: "macobject",
      size: size || 100,
      body: {
             "query" :{
                "filtered": {
                   "filter": {
                     "bool" : {
                      "must" : [
                           {"range": {
                              "timeEnd": {
                                  "gt": timeStart,
                                  "lt": timeEnd
                              }
                          }},
                          {
                          "term": {
                              "userId": userId
                          }
                        }
                    ]
                }
              }
            }
          },
          "sort" : sort
        }
    }));
  },

  getMacAddressFromTabelByLocation: function (location, radius, size) {
    radius = radius || 0;

    return Promise.resolve(client.search({
      index: "mactabels",
      type: "mactabel",
      size: size || 100,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                      "must": [
                          {"geo_bounding_box": {
                            "type":    "indexed",
                            "location": { 
                                 "top_left": {
                                    "lat": location._source.location.lat + BBOX_EDGE[radius],
                                    "lon": location._source.location.lon - BBOX_EDGE[radius]
                                  },
                                  "bottom_right": {
                                    "lat":  location._source.location.lat - BBOX_EDGE[radius],
                                    "lon": location._source.location.lon + BBOX_EDGE[radius]
                                  }
                            }
                          }}
                      ]
                  }
              }
          }
        }
      }
    }));
  },

  loadMessages: function(userId, otherUserId, newerThan, skip, size) {
    var must1 = [
      {"term":  {"user1.userId": userId}},
      {"term":  {"user2.userId": otherUserId}},
    ];
    var must2 = [
      {"term":  {"user2.userId": userId}},
      {"term":  {"user1.userId": otherUserId}},
    ];
    var sort = [
      { "createdAt":   { "order": "desc" }}
    ];
    if (newerThan) {
      must1.push({"range": {
                      "createdAt": {
                          "gt": newerThan
                      }
                  }});
      must2.push({"range": {
                      "createdAt": {
                          "gt": newerThan
                      }
                  }});
    }

    return Promise.resolve(client.search({
      index: "messages",
      type: "message",
      size: size || 20,
      from: skip || 0,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                    "should": [
                      {"bool": {
                        "must": must1,
                      }},
                      {"bool": {
                        "must": must2,
                      }}
                    ]
                  }
              }
          }
        },
        "sort": sort
      }
    }));
  }
}


module.exports = db;
