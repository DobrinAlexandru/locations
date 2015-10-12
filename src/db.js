var _ = require('underscore');
var Promise = require("bluebird");
var uuid = require('node-uuid');

var SEPARATOR = "::";

var db = {
  saveListToDB: function(objects, className, options, bucket) {
    var saveTasks = _.map(objects, function(object) {
      return db.saveObjectToDB(object, className, options, bucket);
    }.bind(this));
    return Promise.settle(saveTasks);
  },
  saveObjectToDB: function(object, className, options, bucket) {
    options = options || {};
    this.assignDBModelInfo(object, className);
    return new Promise(function(resolve, reject) {
      bucket.upsert(object["objectId"], object, options, function(error, result) {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  },

  savePointerToDB: function(pointerType, fromId, toId, options, bucket) {
    var object = {
      objectId: pointerType + SEPARATOR + fromId,
      toId: toId
    };
    return this.saveObjectToDB(object, "Pointer", options, bucket);
  },
  assignDBModelInfo: function(object, className) {
    if (!object["objectId"]) {
      object["objectId"] = uuid.v1();
    }
    object["docType"] = className;
  },

  fetchMultiObjects: function(ids, bucket) {
    return new Promise(function(resolve, reject) {
      if (ids.length === 0) {
        // otherwise, bucket.getMulti will throw an exception
        resolve([]);
      } else {
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
      }
    });
  },
  fetchObject: function(id, bucket) {
    return new Promise(function(resolve, reject) {
      bucket.get(id, function(error, result) {
        var object = result && result.value;
        resolve(object);
      });
    });
  },
  fetchPointer: function(pointerType, fromId, bucket) {
    var id = pointerType + SEPARATOR + fromId;
    return this.fetchObject(id, bucket).bind(this).then(function(result) {
      if (result) {
        return this.fetchObject(result["toId"] || "", bucket);
      } else {
        return Promise.resolve(null);
      }
    });
  },
  executeQuery: function(query, bucket) {
    return new Promise(function(resolve, reject) {
      bucket.query(query, function(error, data, meta) {
        resolve(data);
      });
    });
  },
};

module.exports = db;
