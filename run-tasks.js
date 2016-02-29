'use strict';

var Promise = require('bluebird');
var postgres = Promise.promisifyAll(require('pg'));
var SubscriberTasks = require('./data_tasks/subscribers.js');

var connectionString = 'postgres://localhost/illuminate';
postgres.connectAsync(connectionString)
  .then(function(client, done) {
    if (!client) {
      throw new Error;
    }

    var subscriberTasks = new SubscriberTasks(client);
    subscriberTasks.queryTotal();

    var date = new Date();
    var year = date.getFullYear();
    var month = date.getMonth() + 1;
    month = month < 10 ? '0' + month : month;
    var day = date.getDate();
    var today = year + '-' + month + '-' + day;
    subscriberTasks.queryNewSubscribers(today, '7 days');
    subscriberTasks.queryNewSubscribers(today, '1 month');
    subscriberTasks.queryNewSubscribers(today, '3 months');
  })
  .catch(function(err) {
    console.log(err);
  });
