var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');

var Utils = {
  C: {
    HOUR: 3600000,
    DAY: 8640000,
    YEAR: 31536000000,
    // LOCATIONS_IP: "http://78.47.91.93:8001"
    LOCATIONS_IP: "http://localhost:8001"
  },
  keys: function(a, b) {
    if (a < b) {
      return a + b;
    } else {
      return b + a;
    }
  },
  age: function(birthday) {
    return parseInt((Date.now() - birthday) / Utils.C.YEAR);
  },
  birthday: function(age) {
    return Date.now() - age * Utils.C.YEAR;
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
  },

  genderFilterMatch: function(user1, user2) {
    // Gender filter
    // (x, y) <=> (y, x) || (y, 3)
    // (x, 3) <=> (-, x) || (-, 3)
    // (3, x) <=> (x, 3)
    // (3, 3) <=> (-, 3)
    if (user1._source.gender === 3 && user1._source.genderInt === 3) {
        return  user2._source.genderInt === 3;
      } else if (user1._source.gender === 3) {
        return  user2._source.gender === user1._source.genderInt &&
                user2._source.genderInt === 3;
      } else if (user1._source.genderInt === 3) {
        return  user2._source.genderInt === 3 ||
                user2._source.genderInt === user1._source.gender;
      } else {
        return  user2._source.gender === user1._source.genderInt && (
                  user2._source.genderInt === 3 ||
                  user2._source.genderInt === user1._source.gender
                );
      }
  },
  ageFilterMatch: function(user1, user2) {
    if (!user1._source.birthday || !user2._source.birthday) {
      return false;
    }
    // User1 falls in user 2 interest
    return  user2._source.ageIntMax >=  Utils.age(user1._source.birthday) &&
            user2._source.ageIntMin <=  Utils.age(user1._source.birthday) &&
    // User2 falls in user1 interest
            user2._source.birthday >= Utils.birthday(user1._source.ageIntMax) &&
            user2._source.birthday <= Utils.birthday(user1._source.ageIntMin);
  },
  allFiltersMatch: function(user1, user2) {
    return  Utils.genderFilterMatch(user1, user2) &&
            Utils.ageFilterMatch(user1, user2);
  },
  getFbIdListFromListOfUsers: function(users) {
     return _.map(users, function(user) {
        return user._source.fbid;
    });
  }
};

module.exports = Utils;