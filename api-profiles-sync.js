'use strict';

var Promise = require('bluebird');
var request = require('request');
var xml2js = Promise.promisifyAll(require('xml2js'));
var pg = Promise.promisifyAll(require('pg'));
var argv = require('minimist')(process.argv.slice(2));

/**
 * If process is run with --help, show this.
 */
if (argv.help) {
  console.log();
  console.log('  --local  Sync messages data to the local db');
  console.log('  --aws    Sync messages data to the AWS db');
  console.log();
  process.exit(0);
}

// Time the script starts
var startTime = (new Date()).getTime();

// Helper var indicating there are no more profiles to sync from the API
var apiSyncDone = false;

// If set, is added as the `from` query param to the API call
var queryDate;

// Helper var tracking how many have been read from the API
var profilesRead = 0;

// Helper var tracking how many have been processed by the DB
var profilesProcessed = 0;

// DB client and connection string
var dbClient;
var dbTableName = 'profiles';
var dbConnConfig = {
  database: 'illuminate'
};


if (argv.local) {
  console.log('Syncing to local db...\n');
  dbConnConfig.host = 'localhost';
}
else if (argv.aws) {
  console.log('Syncing to AWS db...\n');

  if (typeof process.env.SHINE_API_SYNC_USER === 'undefined' ||
      typeof process.env.SHINE_API_SYNC_PASSWORD === 'undefined' ||
      typeof process.env.SHINE_API_SYNC_HOST === 'undefined') {
    console.log('Environment vars for AWS DB user, password and host must be set.');
    process.exit(1);
  }

  dbConnConfig.user = process.env.SHINE_API_SYNC_USER;
  dbConnConfig.password = process.env.SHINE_API_SYNC_PASSWORD;
  dbConnConfig.host = process.env.SHINE_API_SYNC_HOST;
  dbConnConfig.port = process.env.SHINE_API_SYNC_PORT || 5432;
  dbConnConfig.ssl = process.env.SHINE_API_SYNC_SSL || true;
}
else {
  console.log('No destination flag specified. Defaulting to --local');
  dbConnConfig.host = 'localhost';
}

// Get a pg client from the connection pool
pg.connectAsync(dbConnConfig)
  .then(function(client) {
    dbClient = client;

    let query = 'SELECT * FROM ' + dbTableName +
      ' ORDER BY updated_at DESC' +
      ' LIMIT 1';
    dbClient.queryAsync(query)
      .then(function(result) {
        if (typeof result !== 'undefined' && typeof result.rows !== 'undefined'
            && result.rows.length > 0) {
          let latest = result.rows[0].updated_at;
          let latestDate = new Date(latest);

          let month = latestDate.getMonth() + 1;
          let date = latestDate.getDate();
          let year = latestDate.getFullYear();
          queryDate = year + '-' + month + '-' + date + 'T00:00:00';
        }

        getProfiles(1, queryDate);
      })
      .catch(function(err) {
        console.log(err);
        process.exit(1);
      });
  })
  .catch(function(err) {
    console.log(err);
    process.exit(1);
  });

var baseUrl = 'https://secure.mcommons.com/api/profiles';
var options = {
  'auth': {
    'user': process.env.MC_AUTH_USER || '',
    'pass': process.env.MC_AUTH_PASSWORD || ''
  }
};

/**
 * API request to get a page of profiles.
 *
 * @param page Specifies page to query for
 * @param date String Time to start querying for messages. ISO-8601
 */
var getProfiles = function(page, date) {
  var url = baseUrl + '?limit=1000&page=' + page;
  if (date) {
    url += '&from=' + date;
  }
  console.log('REQUEST: %s', url);
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

          let columnsFormat = 'id, phone_number, fname, source, ' +
            'opted_out_source, city, state, created_at, updated_at';
          let valuesFormat = '$1, $2, $3, $4, $5, $6, $7, $8, $9';
          let values = [id, phone, first, sourceType, outSource, city, state,
            createdAt, updatedAt];

          if (outAt.length > 0) {
            values[values.length] = outAt;
            columnsFormat += ', opted_out_at';
            valuesFormat += ', $' + values.length;
          }

          // Parse custom columns and any we're interested in
          for (let j = 0; j < profile.custom_columns[0].custom_column.length; j++) {
            let cc = profile.custom_columns[0].custom_column[j];
            let ccName = cc.$.name;
            let ccValue = typeof cc._ === 'string' ? cc._.trim() : null;

            let addValue = false;
            let dbColumnName = '';
            if (ccName === 'alt_source' && ccValue !== null) {
              dbColumnName = 'alt_source';
            }
            else if (ccName === 'Birthday' && ccValue !== null) {
              dbColumnName = 'birthday';
            }
            else if (ccName === 'niche_college_major' && ccValue !== null) {
              dbColumnName = 'college_major';
            }

            if (dbColumnName.length > 0) {
              values[values.length] = ccValue;
              columnsFormat += ', ' + dbColumnName;
              valuesFormat += ', $' + values.length;
            }
          }

          // Setup the query and data for the callback
          let query = 'INSERT INTO ' + dbTableName +
              ' (' + columnsFormat + ')' +
              ' VALUES (' + valuesFormat + ')';

          let callbackData = {
            id: id,
            values: values,
            columnsFormat: columnsFormat,
            valuesFormat: valuesFormat,
          };

          // Insert values into the database
          dbClient.query(query, values, onDbQuery.bind(callbackData));
        }

        // Get the next page of profiles
        page++;
        getProfiles(page, queryDate);
      });
  }
  else {
    console.log('Abort on error code: ' + response.statusCode);
    process.exit(1);
  }
};

/**
 * DB insert query callback.
 *
 * @param err
 * @param result
 */
var onDbQuery = function(err, result) {
  if (err) {
    // 23505 - duplicate key error. Update the row instead.
    if (err.code === '23505' && typeof this.columnsFormat !== 'undefined'
      && typeof this.valuesFormat !== 'undefined'
      && typeof this.values !== 'undefined'
      && typeof this.id !== 'undefined') {

      let query = "UPDATE " + dbTableName +
        " SET (" + this.columnsFormat + ")" +
        " = (" + this.valuesFormat + ")" +
        " WHERE id = '" + this.id + "'";

      dbClient.query(query, this.values, onDbQuery);
    }
    else {
      console.log(err);
      process.exit(1);
    }
  }
  else {
    profilesProcessed++;

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