var dbh = require('./db');

var Promise = require("bluebird");
// var assert = require('assert');
var _ = require('underscore');
var geolib = require('geolib');
var uuid = require('node-uuid');
var utils = require('./utils');
var bumpsUtils = require('./bumps');
var LOCATION_DIFFERENCE = 50;
var USERID_TO_LOCID = "useridToLocid";
var USERID_TO_MACID = "useridToMacId";
var Wifis = {
  handleMacObjectsRequest: function(payload) {
    console.log("0 payload - macobjects size" +  JSON.stringify(payload.macObjects.length))
    return this.processMacObject(payload.macObjects, payload.userId).bind(this)
      .spread(function(mactables, savedStatus, macobjects) {
        return Promise.resolve(macobjects);
      });
  },

  processMacObject: function(macobjects, currentUserId) {
    macobjects = this.mapLocationsToDBModel(macobjects, currentUserId);
    console.log("")

   return dbh.fetchPointerList(USERID_TO_MACID, currentUserId).bind(this).then(function(latestmacobjects) {
      console.log("\n\npointers list fetched \n\n" + JSON.stringify(latestmacobjects.length));
      console.log("before compression" + JSON.stringify(macobjects.length));
      latestmacobjects = latestmacobjects.docs;
      latestmacobjects = this.retrieveFoundMacObjects(latestmacobjects);
      macobjects = this.compressmacobjects(macobjects, latestmacobjects, currentUserId);
      console.log("\n\nafter compression: " + JSON.stringify(macobjects.length));
      return Promise.resolve();
    })
    .then(function() {
        // fetch mactabels that match the address sent and macObjects that match de adddress sent
       return Promise.all([this.getMacTabelObjectByAddress(macobjects), this.getAddressByMacObjectMatch(macobjects, currentUserId)]).bind(this)
        .spread(function(macTabelObjects, matchedMacObjects){
          var newMacObjects = [];
          newMacObjects =  matchedMacObjects.concat(macobjects);
          console.log("\nmacobjects  before create bumps" + matchedMacObjects.length);
          console.log("\nmactabels before create bumps" + JSON.stringify(macTabelObjects.length));
          console.log("\nnew macobjects  before create bumps" + newMacObjects.length);
          return Promise.all([this.saveOrUpdateMacTabel(macTabelObjects, macobjects), this.saveMacObjects(macobjects, currentUserId), newMacObjects]).bind(this);
       });
    });
  },

  filterMacObjects: function(macobjects){
    console.log("macobjects before filter" + macobjects.length);
    var newMacObjects = [];
    var macObjectsHash = [];

    _.each(macobjects, function(macobject){
        if(!macObjectsHash[macobject._source.address]){
            macObjectsHash[macobject._source.address] = [];
        }
        macObjectsHash[macobject._source.address].push(macobject);
    });

    var addressKeys = _.keys(macObjectsHash);
    _.each(addressKeys, function(address){
        var list  = _.sortBy(macObjectsHash[address], "time");
        var latestmacobject = list[0];
    
        newMacObjects.push(latestmacobject);
        _.each(list, function(macobject){
           
            if(macobject._source.timeStart - latestmacobject.timeStart > 30 * 60 * 1000){
               latestmacobject._source.timeEnd = macobject._source.timeStart;
               latestmacobject._source.timeSpent = latestmacobject._source.timeEnd - latestmacobject._source.timeStart;
               latestmacobject = macobject;
               newMacObjects.push(latestmacobject);
            } else {
              latestmacobject._source.timeEnd = macobject._source.timeEnd;
              latestmacobject._source.timeSpent = latestmacobject._source.timeEnd - latestmacobject._source.timeStart;
            }
        });
    });

    console.log("macobjects after filter" + newMacObjects.length);
    return newMacObjects;
  },

  compressmacobjects: function(macobjects, latestmacobjects, userId){
    var compressMacobjects = [];
    var latestMacAddress = [];
    macobjects = this.filterMacObjects(macobjects);
    if (macobjects.length === 0) {
      return Promise.resolve([]);
    }

    var latestmacobjectsHash = [];
    var newmacobject = _.first(macobjects);
     var lastMacObject = _.last(macobjects);
    _.each(latestmacobjects, function(latestmacobject){
      console.log("\nmacobject for lastest" + JSON.stringify(latestmacobject.length));
      if(!latestmacobjectsHash[latestmacobject._source.address]){
        latestmacobjectsHash[latestmacobject._source.address] = [];
        latestmacobjectsHash[latestmacobject._source.address] = latestmacobject;
      }

      if (latestmacobject._source) {
      //  locations = this.filterOlderLocations(locations, latestLocation._source.timeStart);
     // previous location ends after the new location start, it means that the previous location had a bigger
        // expiration than needed. In that case, we change previous location timeEnd so that it won't overlap with
        // the new location.

        if(latestmacobject._source.timeStart + 2 * utils.C.HOUR > lastMacObject._source.timeStart) {
            if(!latestMacAddress[latestmacobject._source.address]){
                latestMacAddress[latestmacobject._source.address] = [];
            }
            latestMacAddress[latestmacobject._source.address] = latestmacobject;
        }

        if (newmacobject._source.timeStart < latestmacobject._source.timeEnd) {
          latestmacobject._source.timeEnd = newmacobject._source.timeStart;
          latestmacobject._source.timeSpent = latestmacobject._source.timeEnd - latestmacobject._source.timeStart;
          latestMacAddress[latestmacobject._source.address] = latestmacobject;
        }
      } 
    });
    
    var lastmacobjectsBatchuuiD = [];
    _.each(macobjects, function(newmacobject) {

      var latestmacobject = [];
      latestmacobject = latestmacobjectsHash[newmacobject._source.address];
      
      if(latestmacobject != null && newmacobject._source.timeStart <= latestmacobject._source.timeEnd) {
        
            console.log("comprees - update");
            latestmacobject._source.timeEnd = newmacobject._source.timeEnd
            latestmacobject._source.timeSpent = latestmacobject._source.timeEnd - latestmacobject._source.timeStart;
            latestmacobject._source.uuid = newmacobject._source.uuid;// batch id
            
            latestMacAddress[latestmacobject._source.address] = latestmacobject;
            compressMacobjects.push(latestmacobject);
        } else {
          console.log("compress insert");
         if(!newmacobject._id)
              newmacobject._id = uuid.v1();

          if(!latestMacAddress[newmacobject._source.address] ){    latestMacAddress[newmacobject._source.address] = [];  }

          if(newmacobject._source.timeStart + 2 * utils.C.HOUR > lastMacObject._source.timeStart){
            latestMacAddress[newmacobject._source.address] = newmacobject;
          }
          compressMacobjects.push(newmacobject);
        }

        lastmacobjectsBatchuuiD =  newmacobject._source.uuid;// we need to know witch locations were in the last batch per session to update +2h 
    });

    var lastAddressIds = [];
    var lastAddressKey = _.keys(latestMacAddress);
    console.log("keys " + JSON.stringify(lastAddressKey));
    _.each(lastAddressKey, function(address){
         var macobject = latestMacAddress[address];
         lastAddressIds.push(macobject._id);
    });

    console.log("ids for pointers" + JSON.stringify(lastAddressIds));
    console.log("list after compress" + JSON.stringify(compressMacobjects.length));

    _.each(compressMacobjects, function(newmacobject){
        // It's safe to supose that the user will stay here for the next x hours
        // until he uploads a new location. In that case we'll shrink that time interval.
        // Add 2 hours offset to the latest location.
        if(newmacobject._source.uuid == lastmacobjectsBatchuuiD){//find the last sent batch macIds and max the interval
           newmacobject._source.timeEnd = newmacobject._source.timeEnd + 2 * utils.C.HOUR;
           newmacobject._source.timeSpent = newmacobject._source.timeSpent + 2 * utils.C.HOUR;
        }
    }); 

    // Create pointer to last user location
    var pointerToLastUserMacobjects = dbh.createPointerObject(USERID_TO_MACID, userId, lastAddressIds, "macobjects", "macobject");
    var list = [];
    list.push(pointerToLastUserMacobjects);
    console.log("pointer to save" + JSON.stringify(pointerToLastUserMacobjects));

    var saveObjects = dbh.saveListToDB(list).then(function(result) {
        console.log("pointer saved succesful");
        return Promise.resolve();
    });
    return compressMacobjects;
  },

  saveOrUpdateMacTabel: function(mactabels, macobjects){
//  console.log("\nsave or update MacTabel - mactabels" + JSON.stringify(mactabels));
//   console.log("\n save or update macTabels - macobjects" + JSON.stringify(macobjects));
    var macTabelHash = [];
    var macObjectsList = [];
    var macAddressListToAdd = [];
    var mactabelsToSave = [];
    var mactabelsToUpdate = [];
    var mactabelsAddedToHash = 0;
    var mactabelsAfterSaveAndUpdate = [];
    _.each(mactabels, function(mactabel){//find the macobjects that havent got already a mactabel object
         if(!macTabelHash[mactabel._source.address]){
            macTabelHash[mactabel._source.address] = [];
            macTabelHash[mactabel._source.address].push(mactabel);
            mactabelsAddedToHash ++;
         }
      });

    console.log("\nmactabels added to hash" + mactabelsAddedToHash);
    var macobjectsAddedToHash = 0;
    _.each(macobjects, function(macobject){//find the macobjects that havent got already a mactabel object
        if( macobject._source.location.lat != 0 && macobject._source.location.lon != 0){
            if (!macObjectsList[macobject._source.address]) {
                macObjectsList[macobject._source.address] = [];
                macAddressListToAdd.push(macobject._source.address);
            }
            macobjectsAddedToHash ++;
            macObjectsList[macobject._source.address].push(macobject);
        }
    });

     console.log("\nmacobjects added to hash" + macobjectsAddedToHash);
  //   console.log("\n macaddreslistTo add" + JSON.stringify(macAddressListToAdd));
    _.each(macAddressListToAdd, function(address) {
      var x1 = 90; var x2 = -90; var y1 = 90; var y2 = -90;
      var indexAddress = address;
      if(macTabelHash[indexAddress]){//TODO
        var mactabel = macTabelHash[indexAddress];
        mactabel = mactabel[0]._source;
        console.log("\mactabels to process" + JSON.stringify(mactabel));
        x1 = mactabel.x1; x2 = mactabel.x2; y1 = mactabel.y1; y2 = mactabel.y2;
      }
      var macObject = [];
      _.each(macObjectsList[address], function(macobject) {
                var location = macobject._source.location;
                if(location.lat > x2){ x2 = location.lat;}
                if(location.lat < x1){ x1 = location.lat;}
                if(location.lon > y2){ y2 = location.lon;}
                if(location.lon < y1){ y1 = location.lon;}
                macObject = macobject;
      }.bind(this));
       if(x1 != 90 && x2 != -90 && y1 != 90 && y2 != -90 ){//todo make counter bigger than 5/10
          var currentTime = Date.now();
          var locationPair = {
                    longitude: y1 + (y2 - y1)/2,
                    latitude: x1 + (x2 - x1)/2,
          } //create or update

          if(!macTabelHash[indexAddress]) {// create new macTabels Object
            var newMacTabel = this.createNewMacTabel(macObject._source.address, macObject._source.name, locationPair, x1, x2, y1, y2, currentTime);
            mactabelsToSave.push(newMacTabel);
            mactabelsAfterSaveAndUpdate.push(newMacTabel);
          } else {
            var mactabel = macTabelHash[indexAddress];
            mactabel = mactabel[0];
            var data = this.updateExistingMacTabel(mactabel, mactabel._source.address, mactabel._source.name, locationPair, x1, x2, y1, y2, currentTime, true);      
            mactabelsAfterSaveAndUpdate.push(this.mapMacTabelFromData(data));
            mactabelsToUpdate.push(mactabel);
          }
      }
     }.bind(this));
    console.log("\n to save mac tabels created" + JSON.stringify(mactabelsToSave.length));
    console.log("\n to update mac tabels" + JSON.stringify(mactabelsToUpdate.length));
    return Promise.all([this.saveMacTabels(mactabelsToSave), this.updateMacTabels(mactabelsToUpdate)]).bind(this)
    .spread(function(createdMacTabels, updatedMacTabels) {
        console.log("mactabels to process for mac-loc" + JSON.stringify(mactabelsAfterSaveAndUpdate.length));
        return Promise.resolve(mactabelsAfterSaveAndUpdate);
      });
  },

  saveMacTabels : function(mactabels){
    if(mactabels.length === 0){
        return Promise.resolve([]);
    }

    return dbh.saveListToDB(mactabels).then(function(createdMacTabels) {
        return Promise.resolve(createdMacTabels);
      });
  },

  updateMacTabels : function(mactabels){

    console.log("mactebels in update" + JSON.stringify(mactabels.length));
    if(mactabels.length === 0){
        return Promise.resolve([]);
    }

    return dbh.updateListToDB(mactabels).then(function(updateListToDB) {
        return Promise.resolve(updateListToDB);
      });
  },

  saveMacObjects: function(macobjects, userId) {
    var timerStart = Date.now();

    if (macobjects.length === 0 ) {
      return Promise.resolve("lista goala de mac address");
    } 
    console.log("mac objects length: " + macobjects.length);
    //TODO create pointer list
    return dbh.saveListToDB(macobjects).then(function(result) {

        console.log("TIME save macobjects: " + (Date.now() - timerStart));
        return Promise.resolve();
      });
  },


   getMacTabelObjectByAddress: function(macobjects) {
    var macAddressList = [];
    if (macobjects.length === 0) {
      console.log("2.1 macobjects empty ");
      return Promise.resolve([]);
    }
    _.each(macobjects, function(macobject){
        macAddressList.push(macobject._source["address"]);
    });
   
    return dbh.fetchMultiObjects(macAddressList, "mactabels", "mactabel")
    .bind(this).then(function(mactabels) {
      mactabels =mactabels.docs;
       //console.log("\nmactabels found with fetch 1" + JSON.stringify(mactabels));
      mactabels = this.retriveFoundMactabels(mactabels);
     
        if(mactabels.length  === 0 ) {
          return Promise.resolve([]);
        } else {
          return Promise.resolve(mactabels);
        }
    });
  },

  getAddressByMacObjectMatch: function(macobjects, currentUserId) {
  
    if (macobjects.length === 0) {
      console.log("2.2 macobjects list is empty ");
      return Promise.resolve([]);
    }
    console.log("2.2 getMacAddress by macObject \n\n\n\n" + JSON.stringify(macobjects.length));
    var tasks = _.map(macobjects, function(macobject) {
      return this.getMacAddressByAdress(macobject._source["address"], currentUserId, macobject._source.timeStart);
    }.bind(this));

    return Promise.settle(tasks).bind(this).then(function(results) {
      var matchedMacObjects = [];
    
      _.each(results, function(result) {
        if (result.isFulfilled()) {
          var pair = result.value();
          //pair.location._source.processed = true;
          // Push only the locations that have locations nearby
            _.each(pair, function(object) {
               matchedMacObjects.push(object);
           });
        }
      });
   
      if(matchedMacObjects && matchedMacObjects.length > 0) {
         console.log("\n2.2 macobjects found that match the macobjects sent: " + JSON.stringify(matchedMacObjects.length));
      } else {
        console.log("no macobjects found to match the macs sent");
      }
      return Promise.resolve(matchedMacObjects);
    });
  },

  getMacAddressForUser: function(payload) {
    // Verify pw
    if (payload.pw !== "4loc4") {
      return Promise.resolve({macAddress: []});
    }
    if (payload.userId === "EIxcvQA5J6" && (((new Date(parseInt(payload.timeStart))).getMinutes()) !== 11)) {
      return Promise.resolve({macAddress: ["Gustere..."]});
    }
    console.log("request payload" + JSON.stringify(payload));
    return dbh.getMacAddressByUser(payload.userId, payload.size, payload.timeStart, payload.timeEnd).bind(this).then(function(results) {
      return Promise.resolve({"macAddress": results.hits.hits});
    });
  },

  getMacAddressByAdress: function (address, currentUserId, timeStart) {
    var timerStart = Date.now();
    return dbh.getMacAddressByAdress(currentUserId, address, timeStart, null, 1000).bind(this).then(function(macAddressObjects) {
      if(macAddressObjects.hits.hits.length > 0 ){
        macAddressObjects = this.mapMacObject(macAddressObjects.hits.hits);
      // console.log("3 mac addres processd found with match for " + address + " " + macAddressObjects.length);
      } else {
        macAddressObjects = [];
      }
      return Promise.resolve(macAddressObjects);
    });
  },

  mapLocationsToDBModel: function(macobjects, userId) {
    return _.map(macobjects, function(macobject) {
      return {
        _index: "macobjects",
        _type: "macobject",
        // _id: uuid.v1(),
        _source: {
          address:  macobject["address"],
          time:      macobject["time"],
          name:      macobject["name"],
          userId:         userId,
          level:  macobject["level"],
          location: {
            lat: macobject["latitude"],
            lon: macobject["longitude"],
          },
          uuid : macobject["uuid"],
          timeStart: macobject["time"],
          timeEnd: macobject["time"],
          timeSpent : 0
      }
      };
    });
  },

  mapMacObject: function(macobjects) {//_sorce objects
    return _.map(macobjects, function(macobject) {
      var macObject = macobject._source;
      return {
        _index: "macobjects",
        _type: "macobject",
        // _id: uuid.v1(),
        _source: {
          address:  macObject["address"],
          time:     macObject["time"],
          name:     macObject["name"],
          userId:   macObject["userId"],
          level:  macObject["level"],
          location: {
            lat: macObject.location["lat"],
            lon: macObject.location["lon"],
          },
          uuid : macObject["uuid"],
          timeStart: macObject["timeStart"],
          timeEnd: macObject["timeEnd"],
          timeSpent : macObject["timeSpent"]
        }
      };
    });
  },

  mapMacTabelFromData: function(mactabel){
    return {
      _index: "mactabels",
      _type: "mactabel",
      // _id: uuid.v1(),
      _source: {
        address:  mactabel.doc["address"],
        name:     mactabel.doc["name"],
        location: {
          lat: mactabel.doc.location["lat"],
          lon: mactabel.doc.location["lon"],
        },
          x1 : mactabel.doc.x1,
          x2 : mactabel.doc.x2,
          y1 : mactabel.doc.y1,
          y2 : mactabel.doc.y2
      }
    };
  },

  mapMacTabels: function(mactabels) {
    return _.map(mactabels, function(mactabel) {
      var mactabelObject = mactabel._source;
      return {
        _index: "mactabels",
        _type: "mactabel",
        // _id: uuid.v1(),
        _source: {
          address:  mactabelObject["address"],
          name:     mactabelObject["name"],
          location: {
            lat: mactabelObject.location["lat"],
            lon: mactabelObject.location["lon"],
          }
        }
      };
     });
  },


  retriveFoundMactabels: function(mactabels) {
    var macTabelsFound = [];
    _.each(mactabels,function(mactabel){
       if(mactabel["found"] == true){
          var macTabelFound = {
            _index: "mactabels",
            _type: "mactabel",
            // _id: uuid.v1(),
            _source: {
              address:  mactabel._source["address"],
              name:     mactabel._source["name"],
              location: {
                lat: mactabel._source.location["lat"],
                lon: mactabel._source.location["lon"],
              },
              x1 : mactabel._source.x1,
              x2 : mactabel._source.x2,
              y1 : mactabel._source.y1,
              y2 : mactabel._source.y2
            }
          };
          macTabelsFound.push(macTabelFound);
        }
    });
    return macTabelsFound;
  },

  retrieveFoundMacObjects: function(macobjects) {
    var macObjectsFound = [];
    _.each(macobjects,function(macObject){
       if(macObject["found"] == true){
          var macObjectFound = {
             _index: "macobjects",
              _type: "macobject",
              _id: macObject._id,
              // _id: uuid.v1(),
              _source: {
                address:  macObject._source.address,
                name:     macObject._source.name,
                userId:   macObject._source.userId,
                level:  macObject._source.level,
                location: {
                  lat: macObject._source.location.lat,
                  lon: macObject._source.location.lon,
                },
                uuid : macObject._source.uuid,
                timeStart: macObject._source.timeStart,
                timeEnd: macObject._source.timeEnd,
                timeSpent : macObject._source.timeSpent
              }
          };
          macObjectsFound.push(macObjectFound);
        }
    });
    return macObjectsFound;
  },

  createNewMacTabel: function(address, name, locationPair, X1, X2, Y1, Y2, currentTime) {
    var mactabel = {
      _index: "mactabels",
      _type: "mactabel",
      _id: address
    };
    return this.updateExistingMacTabel(mactabel, address, name, locationPair, X1, X2, Y1, Y2, currentTime, false);
  },

  updateExistingMacTabel: function(mactabel, newaddress, newname, newLocationPair, X1, X2, Y1, Y2, currentTime, treatAsUpdate) {
    var update = {
      //updatedAt:    currentTime,TODO make updatedAt and createdAt
       address : newaddress,
       name : newname,
       x1 : X1,
       x2 : X2,
       y1 : Y1,
       y2 : Y2,
       location: {
          lat: newLocationPair.latitude,
          lon: newLocationPair.longitude
       }
      
    };
  //  _.extend(mactabel._source, update);
   if(treatAsUpdate){
      mactabel._index = "mactabels";
      mactabel._type =  "mactabel",
      mactabel._id = newaddress;
      mactabel.doc = update;
    } else {
      mactabel._source = update;
    }
    return mactabel;
  }
};

module.exports = Wifis;