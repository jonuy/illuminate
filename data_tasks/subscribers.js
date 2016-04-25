'use strict';

var helpers = require('./helpers.js');

class SubscriberTasks {
  /**
   * Constructor
   *
   * @param client Postgres (pg) client
   */
  constructor(client) {
    this.client = client;
  }

  /**
   * Query for the total number of active subscribers.
   */
  queryTotal() {
    var query = "SELECT COUNT(*) FROM profiles " +
      "WHERE opted_out_source = ''";
    this.client.queryAsync(query, null)
      .then(function(result) {
        console.log('Total # of active subscribers: ' + result.rows[0].count);
      })
      .catch(function(err) {
        console.log(err);
      });
  }

  /**
   * Query for new subscribers by date ranges
   *
   * @param start Date
   * @param interval Range of time prior to start date to query for new
   *                 subscribers. ex: '1 day', '12 weeks', '3 months'
   */
  queryNewSubscribers(start, interval) {
    // @todo Couldn't figure out how to get 'interval' working as a
    // parameterized value.
    var query = "SELECT COUNT(*) FROM profiles " +
      "WHERE created_at <= ($1) " +
      "AND created_at > ($1 - interval '" + interval + "')";
    var values = [start];
    this.client.queryAsync(query, values)
      .then(function(result) {
        console.log('New subscribers from %s and %s prior: %s',
          start, interval, result.rows[0].count);
      })
      .catch(function(err) {
        console.log(err);
      });
  }

  /**
   * Query for active subscribers from week to week. Calculate total and
   * percentage growth and save to a csv ({date}-weekly-growth.csv).
   *
   * @param iterations Number of intervals starting from today to query against
   * @param done Callback on complete
   */
  queryWeeklyGrowth(iterations, done) {
    var counter = 0;
    var startInterval = 1;
    var client = this.client;
    var queryResults = [];

    function runQuery() {
      let tmp = startInterval - counter;
      let queryDate = "date_trunc('week', now()::date + interval '" + tmp + " week')"
      let query = "SELECT " + queryDate + ", COUNT(*) FROM profiles " +
        "WHERE created_at::date < " + queryDate + " " +
        "AND (opted_out_at is null OR opted_out_at::date > " + queryDate + ")";

      client.queryAsync(query)
        .then(function(result) {
          queryResults[queryResults.length] = {
            date: helpers.formatDateForQuery(new Date(result.rows[0].date_trunc)),
            subscribers: parseInt(result.rows[0].count)
          };
          counter++;
          if (counter < iterations) {
            runQuery();
          }
          else {
            onFinish();
          }
        })
        .catch(function(err) {
          console.log(err);
        });
    }

    function onFinish() {
      let labels = ['Date Ending...', 'Subscribers', 'Total Increase', 'Pct Increase'];
      let rows = [];
      for (let i = 0; i < queryResults.length; i++) {
        let resultsIdx = queryResults.length - 1 - i;
        rows[i] = [
          queryResults[resultsIdx].date,
          queryResults[resultsIdx].subscribers,
        ];
        if (i > 0) {
          rows[i][2] = queryResults[resultsIdx].subscribers - queryResults[resultsIdx+1].subscribers;
          rows[i][3] = (rows[i][2] / queryResults[resultsIdx+1].subscribers) * 100;
        }
        else {
          rows[i][2] = '';
          rows[i][3] = '';
        }
      }

      let filename = helpers.formatDateForQuery(new Date()) +
        '-weekly-growth.csv';
      helpers.writeToCsv(labels, rows, filename, done);
    }

    runQuery();
  }

  /**
   * Query for active subscribers from month to month. Calculate total and
   * percentage growth and save to a csv ({date}-monthly-growth.csv).
   *
   * @param iterations Number of intervals starting from today to query against
   * @param done Callback on complete
   */
  queryMonthlyGrowth(iterations, done) {
    var counter = 0;
    var startInterval = 1;
    var client = this.client;
    var queryResults = [];

    function runQuery() {
      let tmp = startInterval - counter;
      let queryDate = "date_trunc('month', now()::date + interval '" + tmp + " month')"
      let query = "SELECT " + queryDate + ", COUNT(*) FROM profiles " +
        "WHERE created_at::date < " + queryDate + " " +
        "AND (opted_out_at is null OR opted_out_at::date > " + queryDate + ")";

      client.queryAsync(query)
        .then(function(result) {
          queryResults[queryResults.length] = {
            date: helpers.formatDateForQuery(new Date(result.rows[0].date_trunc)),
            subscribers: parseInt(result.rows[0].count)
          };

          counter++;
          if (counter < iterations) {
            runQuery();
          }
          else {
            onFinish();
          }
        })
        .catch(function(err) {
          console.log(err);
        });
    }

    function onFinish() {
      let labels = ['Date Ending...', 'Subscribers', 'Total Increase', 'Pct Increase'];
      let rows = [];
      for (let i = 0; i < queryResults.length; i++) {
        let resultsIdx = queryResults.length - 1 - i;
        rows[i] = [
          queryResults[resultsIdx].date,
          queryResults[resultsIdx].subscribers,
        ];
        if (i > 0) {
          rows[i][2] = queryResults[resultsIdx].subscribers - queryResults[resultsIdx+1].subscribers;
          rows[i][3] = (rows[i][2] / queryResults[resultsIdx+1].subscribers) * 100;
        }
        else {
          rows[i][2] = '';
          rows[i][3] = '';
        }
      }

      let filename = helpers.formatDateForQuery(new Date()) +
        '-monthly-growth.csv';
      helpers.writeToCsv(labels, rows, filename, done);
    }

    runQuery();
  }
}

module.exports = SubscriberTasks;