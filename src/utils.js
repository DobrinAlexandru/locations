var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');

var Utils = {
  C: {
    HOUR: 3600000,
    DAY: 8640000,
    YEAR: 31536000000,
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
  }
};

module.exports = Utils;