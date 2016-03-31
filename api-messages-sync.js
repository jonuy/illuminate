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

// Flag indicating if API sync is still in progress
var apiSyncDone = false;

// If set, is added as the `start_time` query param to the API call
var queryDate;

// Helper vars tracking status of messages read from API and inserted into DB
var messagesRead = 0;
var messagesProcessed = 0;

// DB client and connection data
var dbClient;
var dbTableName = 'messages';
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

pg.connectAsync(dbConnConfig)
  .then(function(client) {
    dbClient = client;

    // @todo check if there's a date to start from instead of startin from page 1
    let query = 'SELECT * FROM ' + dbTableName +
      ' ORDER BY received_at DESC' +
      ' LIMIT 1';
    dbClient.queryAsync(query)
      .then(function(result) {
        if (result && result.rows && result.rows.length > 0) {
          let latest = result.rows[0].received_at;
          let latestDate = new Date(latest);

          let month = latestDate.getMonth() + 1;
          let date = latestDate.getDate();
          let year = latestDate.getFullYear();
          queryDate = year + '-' + month + '-' + date + 'T00:00:00';
        }

        getMessages(1, queryDate);
      })
      .catch(function(err) {
        console.log(err);
        process.exit(1);
      });
  })
  .catch(function(err) {
      return;
  });

// API connection info
var baseUrl = 'https://secure.mcommons.com/api/messages';
var options = {
  'auth': {
    'user': process.env.MC_AUTH_USER || '',
    'pass': process.env.MC_AUTH_PASSWORD || ''
  }
};

/**
 * API request to get a page of messages.
 *
 * @param page Number
 * @param date String Time to start querying for messages. ISO-8601
 */
var getMessages = function(page, date) {
  var url = baseUrl + '?limit=1000&page=' + page;
  if (date) {
    url += '&start_time=' + date;
  }
  console.log('REQUEST: ' + url);
  request.get(url, options, onGetMessages);
};

/**
 * Callback for the API request.
 *
 * @param err
 * @param response
 * @param body
 */
var onGetMessages = function(err, response, body) {
  if (err) {
    console.log(err);
    return;
  }

  if (response.statusCode != 200) {
    console.log('Abort on error code: ' + response.statusCode);
    process.exit(1);
  }

  // Convert xml to js object
  xml2js.parseStringAsync(body)
    .then(function(result) {
      let page = parseInt(result.response.messages[0].$.page);

      let msgs = result.response.messages[0].message;
      if (!msgs || msgs.length == 0) {
        apiSyncDone = true;
        finishIfDone();
        return;
      }

      for (let i = 0; i < msgs.length; i++) {
        messagesRead++;

        let messageId = msgs[i].$.id;
        let type = msgs[i].$.type;
        let phone = msgs[i].phone_number[0];
        let body = msgs[i].body[0];
        if (typeof body === 'string') {
          body = body.trim();
        }

        let rcvDate = new Date(msgs[i].received_at[0]);
        let receivedAt = rcvDate.getFullYear() + '-' +
          (rcvDate.getMonth() + 1) + '-' +
          rcvDate.getDate() + 'T' +
          rcvDate.getUTCHours() + ':' +
          rcvDate.getUTCMinutes() + ':' +
          rcvDate.getUTCSeconds();

        let columnsFormat = 'message_id, type, phone_number, body, received_at';
        let valuesFormat = '$1, $2, $3, $4, $5';
        let values = [messageId, type, phone, body, receivedAt];

        let query = 'INSERT INTO ' + dbTableName +
          ' (' + columnsFormat + ')' +
          ' VALUES (' + valuesFormat + ')';

        let callbackData = {
          values: values,
          columnsFormat: columnsFormat,
          valuesFormat: valuesFormat
        };

        // Insert values into the database
        dbClient.query(query, values, onDbInsert.bind(callbackData));
      }

      // Get the next page of messages
      page++;
      getMessages(page, queryDate);
    });
};

/**
 * DB insert query callback.
 *
 * @param err
 * @param result
 */
var onDbInsert = function(err, result) {
  if (err) {
    // 22001 - value too long error. try again with shorter `body`.
    if (err.code === '22001' && typeof this.columnsFormat !== 'undefined'
        && typeof this.valuesFormat !== 'undefined'
        && typeof this.values !== 'undefined') {
      // We're just gonna assume it's the body that's too long. `body` is the
      // 4th item in the values array.
      let values = this.values;
      values[3] = values[3].substr(0, 160);
      let query = 'INSERT INTO ' + dbTableName +
          ' (' + this.columnsFormat + ')' +
          ' VALUES (' + this.valuesFormat + ')';

      console.log('... Trying again with a shortened body value.');
      dbClient.query(query, values, onDbInsert);
    }
    // 23505 - duplicate key error. If it's not that, then error out.
    else if (err.code !== '23505') {
      console.log(err);
      process.exit(1);
    }
    else {
      messagesProcessed++;
      console.log('[%d] %s - %s', messagesProcessed, err.code, err.detail);
    }
  }
  else {
    messagesProcessed++;

    console.log('[%d] %s %d', messagesProcessed, result.command, result.rowCount);
  }

  finishIfDone();
};

/**
 * Clean up and end the process if everything's done.
 */
var finishIfDone = function() {
  if (apiSyncDone && messagesRead == messagesProcessed) {
    console.log('-- DONE --');
    process.exit(0);
  }
}