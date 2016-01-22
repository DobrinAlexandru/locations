var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');

var Utils = {
  C: {
    HOUR: 3600000,
    DAY: 8640000,
    YEAR: 31536000000,
    LOCATIONS_IP: "http://78.47.91.93:8001"
  },
  keys: function(a, b) {
    if (a < b) {
      return a + b;
    } else {
      return b + a;
    }
  },
  getGenderKey: function(gender) {
    switch(gender) {
      case "female": return 1;
      case "male": return 2;
      default: return 3;
    }
  },
  getFirstName: function(name) {
    if(!name) return "";
    var names = name.split(" ");
    return names.length > 1 ? names[0] : name;
  },

  getOtherUsersIds: function(userId, objects) {
    return _.map(objects, function(object) {
        return userId === object._source.user1.userId ? 
                          object._source.user2.userId :
                          object._source.user1.userId;
      });
  }
};

module.exports = Utils;