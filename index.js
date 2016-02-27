'use strict';

var Promise = require('bluebird');
var request = require('request');
var xml2js = Promise.promisifyAll(require('xml2js'));
var util = require('util');

var startTime = (new Date()).getTime();
var profilesRead = 0;

var baseUrl = 'https://secure.mcommons.com/api/profiles';
var options = {
  'auth': {
    'user': '',
    'pass': ''
  }
};

var getProfiles = function(pageNumber) {
  var url = baseUrl + '?page=' + pageNumber;
  request.get(url, options, callback);
}

var callback = function(err, response, body) {
  if (err) {
    console.log(err);
  }

  if (response.statusCode == 200) {
    xml2js.parseStringAsync(body)
      .then(function(result) {
        var numProfiles = parseInt(result.response.profiles[0].$.num);
        var page = parseInt(result.response.profiles[0].$.page);

        if (numProfiles == 0) {
          let endTime = (new Date()).getTime();
          let duration = (endTime - startTime) / 1000;
          console.log('Script time: ' + duration);
          return;
        }

        var profiles = result.response.profiles[0].profile;

        for (var i = 0; i < profiles.length; i++) {
          let profile = profiles[i];
          let first = profile.first_name[0];
          let last = profile.last_name[0];
          let phone = profile.phone_number[0];
          let city;
          let state;
          if (profile.location && profile.location.length > 0) {
            city = profile.location[0].city[0];
            state = profile.location[0].state[0];
          }
          let outAt = profile.opted_out_at[0];
          let outSource = profile.opted_out_source[0];
          let sourceType = profile.source[0].$.type;
          let createdAt = profile.created_at[0];
          let updatedAt = profile.updated_at[0];

          // console.log('-----');
          // console.log('  first: ' + first);
          // console.log('  last: ' + last);
          // console.log('  phone: ' + phone);
          // console.log('  city: ' + city);
          // console.log('  state: ' + state);

          outAt = outAt.length > 0 ? new Date(outAt) : '---';
          // console.log('  outAt: ' + outAt.toString());
          // console.log('  outSource: ' + outSource);
          // console.log('  sourceType: ' + sourceType);

          createdAt = createdAt.length > 0 ? new Date(createdAt) : '---';
          // console.log('  createdAt: ' + createdAt);

          updatedAt = updatedAt.length > 0 ? new Date(updatedAt) : '---';
          // console.log('  updatedAt: ' + updatedAt);

          profilesRead++;
          console.log('profile #: ' + profilesRead);
        }

        page++;
        getProfiles(page);
      });
  }
  else {
    console.log('code: ' + response.statusCode);
  }
};

getProfiles(1);