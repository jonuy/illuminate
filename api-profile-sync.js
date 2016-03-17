'use strict';

var Promise = require('bluebird');
var request = require('request');
var xml2js = Promise.promisifyAll(require('xml2js'));
var pg = require('pg');

// Time the script starts
var startTime = (new Date()).getTime();

// Helper var indicating there are no more profiles to sync from the API
var apiSyncDone = false;

// Helper var tracking how many have been read from the API
var profilesRead = 0;

// Helper var tracking how many have been processed by the DB
var profilesProcessed = 0;

// DB client and connection string
var dbClient;
var dbConnString = 'postgres://localhost/illuminate';
var dbTableName = 'profiles';

// Get a pg client from the connection pool
pg.connect(dbConnString, function(err, client, done) {

  var handleError = function(err) {
    if (! err) return false;

    // An error occurred, remove the client from the connection pool.
    // A truthy value passed to done will remove the connection from the pool
    // instead of simply returning it to be reused.
    // In this case, if we have successfully received a client (truthy)
    // then it will be removed from the pool.
    if (client) {
      done(client);
    }

    console.log('An error occurred');
    return true;
  };

  if (handleError(err)) return;

  // Start API sync at page 1
  dbClient = client;
  getProfiles(1);
});

var baseUrl = 'https://secure.mcommons.com/api/profiles';
var options = {
  'auth': {
    'user': '',
    'pass': ''
  }
};

/**
 * API request to get a page of profiles.
 *
 * @param page Specifies page to query for
 */
var getProfiles = function(page) {
  var url = baseUrl + '?page=' + page;
  request.get(url, options, onGetProfiles);
}

/**
 * Callback for the get-profiles API request.
 *
 * @param err
 * @param response
 * @param body
 */
var onGetProfiles = function(err, response, body) {
  if (err) {
    console.log(err);
  }

  if (response.statusCode == 200) {
    // Convert xml response to js object
    xml2js.parseStringAsync(body)
      .then(function(result) {
        var numProfiles = parseInt(result.response.profiles[0].$.num);
        var page = parseInt(result.response.profiles[0].$.page);

        if (numProfiles == 0) {
          apiSyncDone = true;
          finishIfDone();
          return;
        }

        var profiles = result.response.profiles[0].profile;

        for (var i = 0; i < profiles.length; i++) {
          profilesRead++;

          let profile = profiles[i];
          let id = profile.$.id;
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

          let outAtDate = outAt.length > 0 ? new Date(outAt) : '';
          let createdAtDate = createdAt.length > 0 ? new Date(createdAt) : '';
          let updatedAtDate = updatedAt.length > 0 ? new Date(updatedAt) : '';
          // console.log('  -- first:%s | last:%s | phone:%s | city:%s | state:%s | outAt:%s | outSource:%s | sourceType: %s | createdAt:%s | updatedAt:%s',
          //     first, last, phone, city, state, outAtDate, outSource, sourceType, createdAtDate, updatedAtDate);

          let columnsFormat = 'id, phone_number, source, opted_out_source, city, state';
          let valuesFormat = '$1, $2, $3, $4, $5, $6';
          let values = [id, phone, sourceType, outSource, city, state];

          if (createdAt.length > 0) {
            values[values.length] = createdAt;
            columnsFormat += ', created_at';
            valuesFormat += ', $' + values.length;
          }

          if (outAt.length > 0) {
            values[values.length] = outAt;
            columnsFormat += ', opted_out_at';
            valuesFormat += ', $' + values.length;
          }

          let query = 'INSERT INTO ' + dbTableName +
              ' (' + columnsFormat + ')' +
              ' VALUES (' + valuesFormat + ')';

          // Insert values into the database
          dbClient.query(query, values, onDbQuery);
        }

        // Get the next page of profiles
        page++;
        getProfiles(page);
      });
  }
  else {
    console.log('Abort on error code: ' + response.statusCode);
  }
};

/**
 * DB insert query callback.
 *
 * @param err
 * @param result
 */
var onDbQuery = function(err, result) {
  profilesProcessed++;

  if (err) {
    console.log('[%d] %s', profilesProcessed, err.detail);
  }
  else {
    console.log('[%d] %s %d', profilesProcessed, result.command, result.rowCount);
  }

  finishIfDone();
}

/**
 * Clean up and end the process if everything's done.
 */
var finishIfDone = function() {
  if (apiSyncDone && profilesRead == profilesProcessed) {
    let endTime = (new Date()).getTime();
    let duration = (endTime - startTime) / 1000;
    console.log('-- DONE -- Script time: ' + duration + ' seconds');
    process.exit(0);
  }
};