var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var uuid = require('node-uuid');
var utils = require('./utils');

var Users = {
  updateUser: function(payload) {
    var userId = payload.userId;
    var updateData = payload.updateData;
    var newObject = payload.newObject;

    var data = this.transformParseUserData(updateData);
    var upsertData = _.extend({
      createdAt: data.createdAt ? (new Date(data.createdAt).getTime()) : Date.now(),
      gender: 3,
      genderInt: 3,
    }, data);
    var object = {
      _index: "users",
      _type: "user",
      _id: userId,
      upsert: upsertData,
      doc: data
    };
    return Promise.all([
     this.updateBumpsIfNeeded(userId, data), 
     dbh.updateObjectToDb(object)
    ]).get(1);
  },
  updateUsers: function(userDatas) {
    var promises = _.map(userDatas.updates, function(data) {
      return this.updateUser(data);
    }.bind(this));
    return Promise.all(promises);
  },

  updateBumpsIfNeeded: function(userId, data) {
    if (data.birthday || data.ageIntMin || data.ageIntMax || data.gender || data.genderInt) {
      var update = _.pick(data, "birthday", "ageIntMin", "ageIntMax", "gender", "genderInt");
      // TODO maybe update all bumps with pagination or scroll
      return dbh.loadBumps({userId: userId, sort: true, hidden: true}, true, 0, 2000).bind(this).then(function(bumps) {
        bumps = bumps.hits.hits;
        if (bumps.length < 1) {
          return Promise.resolve({});
        }
        _.each(bumps, function(bump) {
          bump.doc = {
            user2: update
          };
        });
        return dbh.updateListToDB(bumps);
      })
    } else {
      return Promise.resolve({});
    }
  },
  loadUsers: function(payload) {
    var usersIds = payload.usersIds;
    return dbh.fetchMultiObjects(usersIds, "users", "user").bind(this).then(function(users) {
      users = users.docs;
      return Promise.resolve(users);
    });
  },

  transformParseUserData: function(updateData) {
    var object = {
      updatedAt: Date.now()
    };
    _.each(updateData, function(value, key) {
      this.mapping.transform(object, value, key);
    }.bind(this));
    return object;
  },

  mapping: {
    transform: function(object, value, key) {
      if (this[key]) {
        this[key](object, value);
      }
    },
    updatedAt: function(object, value) {
      object.updatedAt = new Date(value).getTime();
    },
    nrOfBumps: function(object, value) {
      object.nrBumps = value;
    },
    feed_loaded_time: function(object, value) {
      object.feedLoadedTime = new Date(value).getTime();
    },
    location: function(object, value) {
      object.location = {
        lat: value.latitude,
        lon: value.longitude
      };
    },
    fbid: function(object, value) {
      object.fbid = value;
    },
    email: function(object, value) {
      object.email = value;
    },
    username: function(object, value) {
      object.username = value;
    },
    authData: function(object, value) {
      object.authData = JSON.stringify(value);
    },
    sessionToken: function(object, value) {
      object.sessionToken = value;
    },
    birthday: function(object, value) {
      object.birthday = new Date(value).getTime();
      var age = utils.age(object.birthday);
      object.ageIntMin = age - 3;
      object.ageIntMax = age + 3;
    },
    gender: function(object, value) {
      object.gender = utils.getGenderKey(value);
    },
    interested_in: function(object, value) {
      object.genderInt = value;
    },
    usr_name: function(object, value) {
      object.name = value;
      object.firstName = utils.getFirstName(value);
    },
    firstName: function(object, value) {
      object.firstName = value;
    },
    is_fake_user: function(object, value) {
      object.isFake = value;
    },
    last_assigned: function(object, value) {
      object.lastTimeFake = value;
    },
    fb_photos: function(object, value) {
      object.fbPhotos = value;
    },
    picture_ratio: function(object, value) {
      object.pictureRatio = value;
    },
    usr_education: function(object, value) {
      object.education = value;
    },
    fb_friends: function(object, value) {
      object.fbFriends = value;
    },
    visible: function(object, value) {
      object.hidden = !value;
    },
  }
};

module.exports = Users;