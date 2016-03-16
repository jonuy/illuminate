'use strict';

var Promise = require('bluebird');
var request = require('request');
var xml2js = Promise.promisifyAll(require('xml2js'));
var pg = Promise.promisifyAll(require('pg'));

// Flag indicating if API sync is still in progress
var apiSyncDone = false;

// Helper vars tracking status of messages read from API and inserted into DB
var messagesRead = 0;
var messagesProcessed = 0;

// DB client and connection data
var dbClient;
var dbConnString = 'postgres://localhost/illuminate';
var dbTableName = 'messages';

pg.connectAsync(dbConnString)
  .then(function(client) {
    dbClient = client;

    // @todo check if there's a date to start from instead of startin from page 1

    getMessages(1);
  })
  .catch(function(err) {
      return;
  });

// API connection info
var baseUrl = 'https://secure.mcommons.com/api/messages';
var options = {
  'auth': {
    'user': '',
    'pass': ''
  }
};

/**
 * API request to get a page of messages.
 *
 * @param page Number
 */
var getMessages = function(page) {
  var url = baseUrl + '?limit=1000&page=' + page;
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
    return;
  }

  // Convert xml to js object
  xml2js.parseStringAsync(body)
    .then(function(result) {
      let page = parseInt(result.response.messages[0].$.page);
      console.log(page);

      let msgs = result.response.messages[0].message;
      if (!msgs || msgs.length == 0) {
        apiSyncDone = true;
        return;
      }

      for (let i = 0; i < msgs.length; i++) {
        messagesRead++;

        let messageId = msgs[i].$.id;
        let phone = msgs[i].phone_number[0];
        let body = msgs[i].body[0];
        let receivedAt = msgs[i].received_at[0];

        let columnsFormat = 'message_id, phone_number, body, received_at';
        let valuesFormat = '$1, $2, $3, $4';
        let values = [messageId, phone, body, receivedAt];

        let query = 'INSERT INTO ' + dbTableName +
          ' (' + columnsFormat + ')' +
          ' VALUES (' + valuesFormat + ')';

        // Insert values into the database
        dbClient.query(query, values, onDbInsert);
      }

      // Get the next page of messages
      page++;
      getMessages(page);
    });
};

/**
 * DB insert query callback.
 *
 * @param err
 * @param result
 */
var onDbInsert = function(err, result) {
  messagesProcessed++;

  if (err) {
    console.log('[%d] %s', messagesProcessed, err.detail);
  }
  else {
    console.log('[%d] %s %d', messagesProcessed, result.command, result.rowCount);
  }

  if (apiSyncDone && messagesRead === messagesProcessed) {
    console.log('-- DONE --');
    // process.exit(0);
  }
};