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
   * Queries for weekly cohorts and how active they are from one week to the next.
   *
   * @param fromDate Date to query back from for cohorts
   * @param done Callback on complete
   */
  cohortInteractionsOverTime(fromDate, done) {
    const NUM_COHORTS = 12;
    const dbClient = this.client;
    var numProcessed = 0;
    var queryResults = [];

    var maxRows = 0;
    var tmp = NUM_COHORTS;
    while (tmp > 0) {
      maxRows += tmp;
      tmp--;
    }

    function theQuery(cohortDate, weekNum) {
      let strDate = helpers.formatDateForQuery(cohortDate);
      let cohortUsers = 0;
      let totalActive = 0; // # of the cohort active from start to the weekNum
      let weeklyUsers = 0;
      let weeklyMessages = 0;

      let conditionCohortUsers =
        "profiles.created_at >= date_trunc('week', date '" + strDate + "') " +
        "AND profiles.created_at < (date_trunc('week', date '" + strDate + "') + interval '1 week')";

      let conditionValidReply =
        "messages.type='reply' AND LOWER(messages.body)=LOWER('M')";

      let conditionReplyTime =
        "messages.received_at <= profiles.created_at + interval '" + weekNum + " week' " +
        "AND messages.received_at > profiles.created_at + interval '" + (weekNum-1) + " week'";

      // @todo Running this more than once is redundant. Consider refactoring so
      // this is only run once.
      let queryUsers = "SELECT COUNT(*) FROM profiles WHERE " + conditionCohortUsers;

      let queryWeeklyUsers = "SELECT COUNT(*) FROM " +
        "(SELECT DISTINCT ON (phone_number) * FROM " +
          "(SELECT profiles.phone_number FROM profiles INNER JOIN messages " +
            "ON profiles.phone_number=messages.phone_number " +
            "WHERE " + conditionCohortUsers + " " +
            "AND " + conditionValidReply + " " +
            "AND " + conditionReplyTime +
          ") AS tmp" +
        ") AS tmp2";

      let queryTotalActive = "SELECT COUNT(*) FROM " +
        "(SELECT DISTINCT ON (phone_number) * FROM " +
          "(SELECT profiles.phone_number FROM profiles INNER JOIN messages " +
            "ON profiles.phone_number=messages.phone_number " +
            "WHERE " + conditionCohortUsers + " " +
            "AND " + conditionValidReply + " " +
            "AND messages.received_at <= profiles.created_at + interval '" + weekNum + " week'" +
          ") AS tmp" +
        ") AS tmp2";

      let queryWeeklyMessages = "SELECT COUNT(*) FROM ( " +
        "SELECT FROM profiles INNER JOIN messages " +
          "ON profiles.phone_number=messages.phone_number " +
          "WHERE " + conditionCohortUsers + " " +
          "AND " + conditionValidReply + " " +
          "AND " + conditionReplyTime +
        ") AS tmp";

      dbClient.queryAsync(queryUsers)
        .then(function(results) {
          cohortUsers = parseInt(results.rows[0].count);
        })
        .then(function() {
          return dbClient.queryAsync(queryWeeklyUsers);
        })
        .then(function(results) {
          weeklyUsers = parseInt(results.rows[0].count);
        })
        .then(function() {
          return dbClient.queryAsync(queryTotalActive);
        })
        .then(function(results) {
          totalActive = parseInt(results.rows[0].count);
        })
        .then(function() {
          return dbClient.queryAsync(queryWeeklyMessages);
        })
        .then(function(results) {
          weeklyMessages = parseInt(results.rows[0].count);

          process.stdout.write('.');

          let useIndex = queryResults.length;
          for (let i = 0; i < queryResults.length; i++) {
            if (queryResults[i].date == strDate) {
              useIndex = i;
              break;
            }
          }

          if (useIndex == queryResults.length) {
            queryResults[useIndex] = {
              date: strDate,
              cohortSize: cohortUsers
            };
          }

          if (typeof queryResults[useIndex].weeklyData === 'undefined') {
            queryResults[useIndex].weeklyData = [];
          }

          let weeklyDataLength = queryResults[useIndex].weeklyData.length;
          queryResults[useIndex].weeklyData[weeklyDataLength] = {
            week: weekNum,
            activeUsers: weeklyUsers,
            totalActive: totalActive,
            pctOfCohort: ((weeklyUsers / cohortUsers) * 100).toFixed(2),
            msgsSent: weeklyMessages,
            avgMsgsSent: (weeklyMessages / weeklyUsers).toFixed(2)
          };

          numProcessed++;
          if (numProcessed == maxRows) {
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

      // First order by cohort date ascending
      queryResults.sort(function(a,b) {
        return new Date(a.date).getTime() - new Date(b.date).getTime();
      });

      // Within each cohort, order by week ascending
      for (let i = 0; i < queryResults.length; i++) {
        queryResults[i].weeklyData.sort(function(a,b) {
          return a.week - b.week;
        });
      }

      let labels = [
        'Week of...',
        'Users in Cohort'
      ];

      for (let i = 1; i <= NUM_COHORTS; i++) {
        labels[labels.length] = 'Week ' + i + ' - Cohort Active Up To Now';
        labels[labels.length] = 'Week ' + i + ' - Active Users';
        labels[labels.length] = 'Week ' + i + ' - % of Cohort';
        labels[labels.length] = 'Week ' + i + ' - Interactions';
        labels[labels.length] = 'Week ' + i + ' - Avg Num Interactions';
      }

      let rows = [];

      for (let i = 0; i < queryResults.length; i++) {
        rows[i] = [
          queryResults[i].date,
          queryResults[i].cohortSize
        ]

        for (let j = 0; j < queryResults[i].weeklyData.length; j++) {
          rows[i][rows[i].length] = queryResults[i].weeklyData[j].totalActive;
          rows[i][rows[i].length] = queryResults[i].weeklyData[j].activeUsers;
          rows[i][rows[i].length] = queryResults[i].weeklyData[j].pctOfCohort;
          rows[i][rows[i].length] = queryResults[i].weeklyData[j].msgsSent;
          rows[i][rows[i].length] = queryResults[i].weeklyData[j].avgMsgsSent;
        }
      }

      let filename = helpers.formatDateForQuery(fromDate) +
        '-cohort-interactions.csv';
      helpers.writeToCsv(labels, rows, filename, done);
    }

    // Get weekly data for the past NUM_COHORTS
    for (let i = 0; i < NUM_COHORTS; i++) {
      let cohortDate = new Date(fromDate.getTime());
      cohortDate.setDate(cohortDate.getDate() - (i * 7));

      // Set date to the Monday of that week
      let day = cohortDate.getDay();
      let diff = cohortDate.getDate() - day + (day == 0 ? -6 : 1); // adjust when day is sunday
      cohortDate.setDate(diff);

      // NUM_COHORTS is also the max number of weeks we can query for
      for (let j = 1; j <= NUM_COHORTS; j++) {
        // ... but no need to query for more weeks than the cohort has existed
        if (j > i + 1) {
          break;
        }

        theQuery(cohortDate, j);
      }
    }
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

    var queryResults = [];

    // Executes queries to get the info we want for each day and stores the
    // results in `queryResults`
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
        })
        .then(function() {
          return dbClient.queryAsync(queryM);
        })
        .then(function(results) {
          totalRepliesM = parseInt(results.rows[0].count);
        })
        .then(function() {
          return dbClient.queryAsync(queryUniqueUsers);
        })
        .then(function(results) {
          totalUniqueUsers = parseInt(results.rows[0].count);
          if (totalUniqueUsers != 0) {
            avgReplies = (totalReplies / totalUniqueUsers).toFixed(2);
          }

          console.log('%s: Total unique users: %d, Avg replies: %d',
            strDate, totalUniqueUsers, avgReplies);

          queryResults[queryResults.length] = {
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
      queryResults.sort(function(a,b) {
        return new Date(b.date).getTime() - new Date(a.date).getTime();
      });

      let labels = [
        'Date',
        'Total Replies',
        '"M" Replies',
        '# of Unique Users',
        'Average # of Replies Per User'
        ];

      let rows = [];
      for (let i = 0; i < queryResults.length; i++) {
        rows[i] = [
          queryResults[i].date,
          queryResults[i].replies,
          queryResults[i].m,
          queryResults[i].users,
          queryResults[i].avgReplies
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

  /**
   * Queries for the total number of users who have been active over a period
   * of time.
   *
   * @param endDate Date to start querying back from
   * @param numIntervals Number of intervals to query
   * @param interval String. ex: 'week'
   * @param done Callback when complete
   */
  totalActiveUsers(endDate, numIntervals, interval, done) {
    var dbClient = this.client;
    var date = helpers.formatDateForQuery(endDate);
    var numProcessed = 0;
    var queryResults = [];

    console.log('Running query for total active users: %s - %d %s',
      endDate, numIntervals, interval);

    function theQuery(range) {
      let query = "SELECT COUNT(*) FROM " +
        "(SELECT DISTINCT ON (phone_number) * FROM messages " +
          "WHERE received_at > " +
            "(date '" + date + "' - interval '" + range + " " + interval + "') " +
          "AND received_at <= date '" + date + "' " +
          "AND type='reply') AS tmp";

      dbClient.queryAsync(query)
        .then(function(results) {
          queryResults[queryResults.length] = {
            'range': range,
            'count': parseInt(results.rows[0].count)
          };
          process.stdout.write('.');

          numProcessed++;
          if (numProcessed == numIntervals) {
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
      // Sort in ascending order
      queryResults.sort(function(a,b) {
        return a.range - b.range;
      });

      let labels = [
        'From ' + date,
        'Users'
        ];

      let rows = [];
      for (let i = 0; i < queryResults.length; i++) {
        rows[i] = [
          'Active in Last ' + queryResults[i].range + ' ' + interval + 's',
          queryResults[i].count
          ];
      }

      let filename = helpers.formatDateForQuery(endDate) +
        '-activeusers-past-' + numIntervals + '-' + interval + '.csv';
      helpers.writeToCsv(labels, rows, filename, done);
    }

    // Execute query over the set number of intervals
    for (let i = 1 ; i <= numIntervals; i++) {
      theQuery(i);
    }
  }

}

module.exports = BehaviorTasks;