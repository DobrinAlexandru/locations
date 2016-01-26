var _ = require('underscore');
var Promise = require("bluebird");
var uuid = require('node-uuid');

var utils = require('./utils');

var SEPARATOR = "::";
// The length of the bounding box half edge(radius) in degrees
// http://msi.nga.mil/MSISiteContent/StaticFiles/Calculators/degree.html
// 1 degree = 110575m
var BBOX_EDGE = [0.001356545, 0.002713090, 0.004521817]  // 150m, 300m, 500m
// 10 minutes overlap to consider that two locations intersect in time
var TIME_OFFSET = 10 * 60000;
// Time interval we search for should not expand more than 1day before & after location time
var TIME_BOUND = 24 * 3600000;

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  // log: 'trace'
  log : [{
    type: 'stdio',
    levels: ['error', 'warning'] // change these options
  }]
});

var db = {
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

  loadBumps: function(userId, otherUsersIds, friendStatus, reverse, skip, size) {
    var term = {};
    term["user" + (!reverse ? "1" : "2") + ".userId"] = userId;

    var must = [
      {"term":  term}
    ];
    if (friendStatus) {
      must.push({"term": {"friendStatus": friendStatus}});
    }
    var sort;
    if (otherUsersIds) {
      var terms = {};
      terms["user" + (reverse ? "1" : "2") + ".userId"] = otherUsersIds
      must.push({"terms": terms});
    } else {
      sort = [
        { "updatedAt":   { "order": "desc" }}
      ];
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
                  "bool": {
                      "must": must,
                      "must_not": {"term": {"friendStatus": 4}}
                  }
              }
          }
        },
        "sort": sort
      }
    }));
  },

  pickAvailableFakeUsers: function(user, size) {
    var currentTime = Date.now();
    return Promise.resolve(client.search({
      index: "users",
      type: "user",
      size: size || 100,
      body: {
        "query": {
          "filtered" : {
              "filter" : {
                  "bool": {
                      "must": [
                          {"range": {
                              "birthday": {
                                  "gt":  currentTime - user._source.ageIntMin * utils.C.YEAR,
                                  "lt":  currentTime - user._source.ageIntMax * utils.C.YEAR
                              }
                          }},
                          {"range": {
                              "lastTimeFake": {
                                  "lt":   currentTime - 20 * 60000
                              }
                          }},
                          {"term": {
                              "isFake": true
                          }},
                      ]
                  }
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
