var _ = require('underscore');
var Promise = require("bluebird");
var uuid = require('node-uuid');

var SEPARATOR = "::";
// The length of the bounding box half edge(radius) in degrees
// http://msi.nga.mil/MSISiteContent/StaticFiles/Calculators/degree.html
// 1 degree = 110575m
var BBOX_EDGE = 0.001356545;  // 150m
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
    var saveTasks = _.map(objects, function(object) {
      return db.saveObjectToDB(object);
    }.bind(this));
    return Promise.settle(saveTasks);
  },
  saveObjectToDB: function(object) {
    return Promise.resolve(client.index({
      index: object._index,
      type: object._type,
      id: object._id,
      body: object._source
    }));
  },

  savePointerToDB: function(pointerType, fromId, id, index, type) {
    var object = {
      _index: "pointers",
      _type: pointerType,
      _id: fromId,
      _source: {
        index: index,
        type: type,
        id: id
      }
    };
    return this.saveObjectToDB(object);
  },

  fetchMultiObjects: function(ids, index, type) {
    return this.fetchMulti({
      index: index,
      type: type,
      body: {
        ids: ids
      }
    });
  },
  fetchObject: function(id, index, type) {
    return this.fetch({
      index: index,
      type: type,
      id: id
    });
  },
  fetchMulti: function(params) {
    params = _.extend(params, {
      ignore: [404]
    });
    return Promise.resolve(client.mget(params));
  },
  fetch: function(params) {
    params = _.extend(params, {
      ignore: [404]
    });
    return Promise.resolve(client.get(params));
  },
  fetchPointer: function(pointerType, fromId) {
    return this.fetch({
      index: "pointers",
      type: pointerType,
      id: fromId
    }).bind(this).then(function(pointer) {
      console.log("pointer yes");
      return pointer._source ? this.fetch(pointer._source) : Promise.resolve({});
    }, function(error) {
      console.log("pointer no" + JSON.stringify(error));
      return Promise.reject(error);
    });
  },
  getLocationsForUserQuery: function(userId, timeStart, timeEnd) {
    return Promise.resolve(client.search({
      index: "locations",
      type: "location",
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
  getLocationsNearSingleLocation: function(location, excludeUserId) {
    return Promise.resolve(client.search({
      index: "locations",
      type: "location",
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
                                    "lat": location._source.location.lat + BBOX_EDGE,
                                    "lon": location._source.location.lon - BBOX_EDGE
                                  },
                                  "bottom_right": {
                                    "lat":  location._source.location.lat - BBOX_EDGE,
                                    "lon": location._source.location.lon + BBOX_EDGE
                                  }
                            }
                          }},
                          {"range": {
                              "timeEnd": {
                                  "gt":  location._source.timeStart - TIME_OFFSET,
                                  "lt":   Math.min(Date.now(), location._source.timeStart + TIME_BOUND)
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
  }
}


module.exports = db;
