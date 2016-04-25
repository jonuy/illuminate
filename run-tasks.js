'use strict';

var Promise = require('bluebird');
var postgres = Promise.promisifyAll(require('pg'));
var BehaviorTasks = require('./data_tasks/behavior.js');
var RetentionTasks = require('./data_tasks/retention.js');
var SubscriberTasks = require('./data_tasks/subscribers.js');
var helpers = require('./data_tasks/helpers.js');
var argv = require('minimist')(process.argv.slice(2));

/**
 * If process is run with --help, show this.
 */
if (argv.help) {
  console.log('Specify tasks with the --task option. By default, all will run.');
  console.log();
  console.log('  ex: node run-tasks.js --task new-subscribers --task total-subscribers');
  console.log();
  console.log('Available tasks:');
  console.log();
  console.log('  active-users        Total active users in recent past');
  console.log('  cohort-analysis     Churn/retentation data');
  console.log('  cohort-interactions Cohort behavior over time');
  console.log('  daily-interactions  # of interactions daily');
  console.log('  pct-interactions    Percentage of subscribers interacting week-to-week/month-to-month');
  console.log('  growth              Total subscriber count from week-to-week/month-to-month');
  console.log('  subscribers         Runs both new-subscribers and total-subscribers');
  console.log('  new-subscribers     Queries for new recent subscribers');
  console.log('  total-subscribers   Total # of current subscribers');
  console.log();
  process.exit(0);
}

/**
 * If the command line `task` arg is set, check if it matches the `name`
 * parameter. Otherwise return true.
 *
 * @param name String
 * @return boolean
 */
function argsEmptyOrSetTo(name) {
  if (typeof argv.task === 'undefined') {
    return true;
  }
  else if (typeof argv.task === 'string') {
    return argv.task == name;
  }
  else if (argv.task instanceof Array) {
    return argv.task.indexOf(name) >= 0;
  }
  else {
    return true;
  }
}

var connectionString = 'postgres://localhost/illuminate';
postgres.connectAsync(connectionString)
  .then(function(client, done) {
    if (!client) {
      throw new Error;
    }

    var date = new Date();
    var today = helpers.formatDateForQuery(date);
    var behaviorTasks = new BehaviorTasks(client);
    var retentionTasks = new RetentionTasks(client);
    var subscriberTasks = new SubscriberTasks(client);

    if (argsEmptyOrSetTo('total-subscribers') || argsEmptyOrSetTo('subscribers')) {
      subscriberTasks.queryTotal();
    }

    if (argsEmptyOrSetTo('new-subscribers') || argsEmptyOrSetTo('subscribers')) {
      subscriberTasks.queryNewSubscribers(today, '7 days');
      subscriberTasks.queryNewSubscribers(today, '1 month');
      subscriberTasks.queryNewSubscribers(today, '3 months');
    }

    if (argsEmptyOrSetTo('growth')) {
      subscriberTasks.queryMonthlyGrowth(7);
      subscriberTasks.queryWeeklyGrowth(20);
    }

    if (argsEmptyOrSetTo('cohort-analysis')) {
      retentionTasks.triangleChart('week', 15);
      retentionTasks.triangleChart('month', 6);
    }

    if (argsEmptyOrSetTo('cohort-interactions')) {
      behaviorTasks.cohortInteractionsOverTime(date);
    }

    if (argsEmptyOrSetTo('daily-interactions')) {
      behaviorTasks.dailyInteractions(date, 14);
    }

    if (argsEmptyOrSetTo('pct-interactions')) {
      behaviorTasks.pctInteractions('month', 7);
      behaviorTasks.pctInteractions('week', 20);
    }

    if (argsEmptyOrSetTo('active-users')) {
      behaviorTasks.totalActiveUsers(date, 9, 'week');
      behaviorTasks.totalActiveUsers(date, 14, 'day');
    }
  })
  .catch(function(err) {
    console.log(err);
  });
