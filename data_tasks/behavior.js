'use strict';

var fs = require('fs');

var helpers = require('./helpers.js');

class BehaviorTasks {
  /**
   * Constructor
   *
   * @param client Postgres (pg) client
   */
  constructor(client) {
    this.client = client;
  }

  /**
   * Queries for general daily user interactions. This will write a csv file that
   * includes the total number of replies, how many of those replies were 'M',
   * the number of unique users who replied, and the average number of replies.
   *
   * @param endDate Date to start querying back from
   * @param range Number of days to query back from the endDate
   * @param done Callback when complete
   */
  dailyInteractions(endDate, range, done) {
    var dbClient = this.client;
    var numProcessed = 0;

    var dataResults = [];

    // Executes queries to get the info we want for each day and stores the
    // results in `dataResults`
    function theDailyQuery(date) {
      let totalReplies = 0;
      let totalRepliesM = 0;
      let totalUniqueUsers = 0;
      let avgReplies = 0;
      let strDate = helpers.formatDateForQuery(date);

      let queryTotal = "SELECT COUNT(*) FROM messages " +
        "WHERE received_at >= date '" + strDate + "' " +
          "AND received_at < (date '" + strDate + "' + interval '1 day') " +
          "AND type='reply'";

      let queryM = "SELECT COUNT(*) FROM messages " +
        "WHERE received_at >= date '" + strDate + "' " +
          "AND received_at < (date '" + strDate + "' + interval '1 day') " +
          "AND type='reply' AND LOWER(body)=LOWER('M')";

      let queryUniqueUsers = "SELECT COUNT(*) FROM " +
        "(" +
          "SELECT DISTINCT ON (phone_number) " +
            "* FROM messages " +
            "WHERE received_at >= date '" + strDate + "' " +
            "AND received_at < (date '" + strDate + "' + interval '1 day') " +
            "AND type='reply'" +
        ") AS tmp";

      dbClient.queryAsync(queryTotal)
        .then(function(results) {
          totalReplies = parseInt(results.rows[0].count);

          console.log(strDate + ': total replies: ' + totalReplies);
        })
        .then(function() {
          return dbClient.queryAsync(queryM);
        })
        .then(function(results) {
          totalRepliesM = parseInt(results.rows[0].count);

          console.log(strDate + ': M replies: ' + totalRepliesM);
        })
        .then(function() {
          return dbClient.queryAsync(queryUniqueUsers);
        })
        .then(function(results) {
          totalUniqueUsers = parseInt(results.rows[0].count);
          if (totalUniqueUsers != 0) {
            avgReplies = (totalReplies / totalUniqueUsers).toFixed(2);
          }

          console.log(strDate + ': Total unique users: ' + totalUniqueUsers);
          console.log(strDate + ': Avg replies: ' + avgReplies);

          dataResults[dataResults.length] = {
            date: strDate,
            replies: totalReplies,
            m: totalRepliesM,
            users: totalUniqueUsers,
            avgReplies: avgReplies
          };

          numProcessed++;
          if (numProcessed == range) {
            onFinish();
          }
        })
        .catch(function(error) {
          console.log(error);
          process.exit(1);
        });
    }

    // Run when all queries are finished processing
    function onFinish() {
      // Sort in descending order
      dataResults.sort(function(a,b) {
        return new Date(b.date).getTime() - new Date(a.date).getTime() ;
      });

      let labels = [
        'Date',
        'Total Replies',
        '"M" Replies',
        '# of Unique Users',
        'Average # of Replies Per User'
        ];

      let rows = [];
      for (let i = 0; i < dataResults.length; i++) {
        rows[i] = [
          dataResults[i].date,
          dataResults[i].replies,
          dataResults[i].m,
          dataResults[i].users,
          dataResults[i].avgReplies
          ];
      }

      let filename = helpers.formatDateForQuery(endDate) +
        '-behavior-past-' + range + '-days.csv';
      helpers.writeToCsv(labels, rows, filename, done);
    }

    // Run the queries for each day
    for (let i = 0; i < range; i++) {
      let tmp = new Date(endDate.getTime());
      tmp.setDate(tmp.getDate() - i);
      theDailyQuery(tmp);
    }
  }

}

module.exports = BehaviorTasks;