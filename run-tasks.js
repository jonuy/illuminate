'use strict';

var Promise = require('bluebird');
var postgres = Promise.promisifyAll(require('pg'));
var BehaviorTasks = require('./data_tasks/behavior.js');
var RetentionTasks = require('./data_tasks/retention.js');
var SubscriberTasks = require('./data_tasks/subscribers.js');
var helpers = require('./data_tasks/helpers.js');

var connectionString = 'postgres://localhost/illuminate';
postgres.connectAsync(connectionString)
  .then(function(client, done) {
    if (!client) {
      throw new Error;
    }

    var subscriberTasks = new SubscriberTasks(client);
    subscriberTasks.queryTotal();

    var date = new Date();
    var today = helpers.formatDateForQuery(date);
    subscriberTasks.queryNewSubscribers(today, '7 days');
    subscriberTasks.queryNewSubscribers(today, '1 month');
    subscriberTasks.queryNewSubscribers(today, '3 months');

    var retentionTasks = new RetentionTasks(client);
    retentionTasks.triangleChart('week', 12);
    retentionTasks.triangleChart('month', 6);

    var behaviorTasks = new BehaviorTasks(client);
    behaviorTasks.dailyInteractions(date, 30);
  })
  .catch(function(err) {
    console.log(err);
  });
