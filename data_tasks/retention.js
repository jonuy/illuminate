'use strict';

var fs = require('fs');

class RetentionTasks {
  /**
   * Constructor
   *
   * @param client Postgres (pg) client
   */
  constructor(client) {
    this.client = client;
  }

  /**
   * Query for retention/churn data by week.
   */
  triangleChartByWeek() {
    var dbClient = this.client;
    var maxWeeks = 12;
    var rows = [];
    var labels = [''];
    for (var i = 0; i <= maxWeeks; i++) {
      if (i == 0) {
        labels[labels.length] = 'New Users';
      }
      else {
        labels[labels.length] = 'Week ' + i.toString();
      }
    }

    // @todo There's probably a better way of finding out if this thing's done
    var hack_counter = 0;
    var hack_num = maxWeeks;
    while (hack_num > 0) {
      hack_counter += hack_num;
      hack_num -= 1;
    }

    function runWeekQuery(weeksAgoStart) {
      var weekString;
      var rowNumber = maxWeeks - weeksAgoStart;
      var queryForWeek = "SELECT date_trunc('week', current_date) - interval '" + weeksAgoStart + " weeks'";
 
      dbClient.queryAsync(queryForWeek)
        .then(function(results) {
          weekString = convertDateToHumanReadable(results.rows[0]['?column?']);
          console.log('Querying for data for week of: %s', weekString);
          rows[rowNumber] = [];
          rows[rowNumber][0] = weekString;

          let createdStart = weeksAgoStart;
          let createdEnd = createdStart - 1;
          var queryForNewSubscribers = "SELECT COUNT(*) FROM profiles " +
            "WHERE created_at >= (date_trunc('week', current_date) - interval '" + createdStart + " weeks') " +
              "AND created_at < (date_trunc('week', current_date) - interval '" + createdEnd + " weeks')";

          return dbClient.queryAsync(queryForNewSubscribers);
        })
        .then(function(results) {
          rows[rowNumber][1] = results.rows[0].count;
          // console.log('  New at %s: ', weekString, results.rows[0].count);

          // Now get the churn for interval
          function runChurn(key, createdStart, createdEnd, outStart, outEnd) {
            var query = "SELECT COUNT(*) FROM profiles " +
              "WHERE created_at >= (date_trunc('week', current_date) - interval '" + createdStart + " weeks') " +
              "AND created_at < (date_trunc('week', current_date) - interval '" + createdEnd + " weeks') " +
              "AND opted_out_at >= (date_trunc('week', current_date) - interval '" + outStart + " weeks') " +
              "AND opted_out_at < (date_trunc('week', current_date) - interval '" + outEnd + " weeks')";

            dbClient.queryAsync(query)
              .then(function(results) {
                var rowColumn = weeksAgoStart - outStart + 2;
                rows[rowNumber][rowColumn] = results.rows[0].count;

                // console.log('    opted out within %d week(s) after %s: %s', maxWeeks - outStart, key, results.rows[0].count);

                outStart--;
                outEnd--;
                if (outStart > 0) {
                  runChurn(key, createdStart, createdEnd, outStart, outEnd);
                }

                // @todo Done. But yea, probably a better way of figuring out if we've reached the end.
                hack_counter--;
                if (hack_counter <= 0) {
                  writeCsvFile(labels, rows, 'week');
                }
              })
              .catch(function(error) {
                console.log(error);
              });
          }

          let cStart = weeksAgoStart;
          let cEnd = cStart - 1;
          let oStart = cStart;
          let oEnd = cStart - 1;
          runChurn(weekString, cStart, cEnd, oStart, oEnd);
        })
        .catch(function(error) {
          console.log(error);
        });
    };

    for (var i = maxWeeks; i > 0; i--) {
      runWeekQuery(i);
    }
  }

  /**
   * Query for retention/churn data by month.
   *
   * @todo This is pretty much just a straight up copy and paste of the
   *   triangleChartByWeek function. This can DEFINITELY be more DRY. But
   *   I'm not about to go do that at 3:40am.
   */
  triangleChartByMonth() {
    var dbClient = this.client;
    var maxMonths = 7;
    var rows = [];
    var labels = [''];
    for (var i = 0; i <= maxMonths; i++) {
      if (i == 0) {
        labels[labels.length] = 'New Users';
      }
      else {
        labels[labels.length] = 'Month ' + i.toString();
      }
    }

    // @todo There's probably a better way of finding out if this thing's done
    var hack_counter = 0;
    var hack_num = maxMonths;
    while (hack_num > 0) {
      hack_counter += hack_num;
      hack_num -= 1;
    }

    function runMonthQuery(monthsAgoStart) {
      var monthString;
      var rowNumber = maxMonths - monthsAgoStart;
      var queryForMonth = "SELECT date_trunc('month', current_date) - interval '" + monthsAgoStart + " months'";
 
      dbClient.queryAsync(queryForMonth)
        .then(function(results) {
          monthString = convertDateToHumanReadable(results.rows[0]['?column?']);
          console.log('Querying for data for month: %s', monthString);
          rows[rowNumber] = [];
          rows[rowNumber][0] = monthString;

          let createdStart = monthsAgoStart;
          let createdEnd = createdStart - 1;
          var queryForNewSubscribers = "SELECT COUNT(*) FROM profiles " +
            "WHERE created_at >= (date_trunc('month', current_date) - interval '" + createdStart + " months') " +
              "AND created_at < (date_trunc('month', current_date) - interval '" + createdEnd + " months')";

          return dbClient.queryAsync(queryForNewSubscribers);
        })
        .then(function(results) {
          rows[rowNumber][1] = results.rows[0].count;
          // console.log('  New at %s: ', monthString, results.rows[0].count);

          // Now get the churn for interval
          function runChurn(key, createdStart, createdEnd, outStart, outEnd) {
            var query = "SELECT COUNT(*) FROM profiles " +
              "WHERE created_at >= (date_trunc('month', current_date) - interval '" + createdStart + " months') " +
              "AND created_at < (date_trunc('month', current_date) - interval '" + createdEnd + " months') " +
              "AND opted_out_at >= (date_trunc('month', current_date) - interval '" + outStart + " months') " +
              "AND opted_out_at < (date_trunc('month', current_date) - interval '" + outEnd + " months')";

            dbClient.queryAsync(query)
              .then(function(results) {
                var rowColumn = monthsAgoStart - outStart + 2;
                rows[rowNumber][rowColumn] = results.rows[0].count;

                // console.log('    opted out within %d month(s) after %s: %s', maxMonths - outStart, key, results.rows[0].count);

                outStart--;
                outEnd--;
                if (outStart > 0) {
                  runChurn(key, createdStart, createdEnd, outStart, outEnd);
                }

                // @todo Done. But yea, probably a better way of figuring out if we've reached the end.
                hack_counter--;
                if (hack_counter <= 0) {
                  writeCsvFile(labels, rows, 'month');
                }
              })
              .catch(function(error) {
                console.log(error);
              });
          }

          let cStart = monthsAgoStart;
          let cEnd = cStart - 1;
          let oStart = cStart;
          let oEnd = cStart - 1;
          runChurn(monthString, cStart, cEnd, oStart, oEnd);
        })
        .catch(function(error) {
          console.log(error);
        });
    };

    for (var i = maxMonths; i > 0; i--) {
      runMonthQuery(i);
    }
  }
}

/**
 * Helper function to convert a Date object to Y-m-d format.
 *
 * @param date Date object
 * @return string
 */
function convertDateToHumanReadable(date) {
  var year = date.getFullYear();
  var month = date.getMonth() + 1;
  var day = date.getDate();
  return year + '-' + month + '-' + day;
}

/**
 * Writes CSV files of the collected triangle chart data.
 *
 * @param labels Array of top row labels
 * @param rows Double array of the data
 * @param interval String label indicating intervals of data
 */
function writeCsvFile(labels, rows, interval) {
  var csvDataRaw = '';
  var csvDataPct = '';
  var csvSummary = '';
  var results = [];
  results[0] = labels;

  for (let i = 0; i < rows.length; i++) {
    results[results.length] = rows[i];
  }

  // console.log('\n\n');
  // console.log(results);

  var summaryData = [];

  for (let i = 0; i < results.length; i++) {
    summaryData[i] = [];
    if (i == 0) {
      summaryData[i][1] = 'Total Retention';
    }

    for (let j = 0; j < results[i].length; j++) {
      let sIndex = j;
      if (j >= 2) {
        sIndex = j + 1;
      }

      csvDataRaw += results[i][j];

      // In this range, we have raw opt-out numbers. Convert to percent instead.
      let pctValue = results[i][j];
      if (i > 0 && j > 1) {
        let rawOut = parseInt(results[i][j]);
        let rawNew = parseInt(results[i][1]);
        pctValue = ((rawOut / rawNew) * 100).toFixed(2);
      }

      csvDataPct += pctValue + '%';
      summaryData[i][sIndex] = pctValue + '%';

      // If not at the end of the row, add a comma
      if (j < results[i].length - 1) {
        csvDataRaw += ',';
        csvDataPct += ',';
      }
      // 
      else if (i != 0) {
        let totalNew = parseInt(results[i][1]);
        let totalOut = 0;
        for (let k = j; k > 1; k--) {
          totalOut += parseInt(results[i][k]);
        }

        let retention = (((totalNew - totalOut) / totalNew) * 100).toFixed(2);
        summaryData[i][2] = retention + '%';
      }
    }
    csvDataRaw += '\n';
    csvDataPct += '\n';
  }

  // Write all the files!
  var date = new Date();
  var dateStr = convertDateToHumanReadable(date);
  var filenameRaw = dateStr + '-raw-by-' + interval + '.csv';
  fs.writeFile('./output/' + filenameRaw, csvDataRaw, function(err) {
    if (err) {
      return console.log(err);
    }

    console.log('The file ' + filenameRaw + ' was saved!');
  });

  var filenamePct = dateStr + '-pct-by-' + interval + '.csv';
  fs.writeFile('./output/' + filenamePct, csvDataPct, function(err) {
    if (err) {
      return console.log(err);
    }

    console.log('The file ' + filenamePct + ' was saved!');
  });


  for (let i = 0; i < summaryData.length; i++) {
    for (let j = 0; j < summaryData[i].length; j++) {
      csvSummary += summaryData[i][j];

      // If not at the end of the row, add a comma
      if (j < summaryData[i].length - 1) {
        csvSummary += ',';
      }
    }
    csvSummary += '\n';
  }

  var filenameSummary = dateStr + '-summary-by-' + interval + '.csv';
  fs.writeFile('./output/' + filenameSummary, csvSummary, function(err) {
    if (err) {
      return console.log(err);
    }

    console.log('The file ' + filenameSummary + ' was saved!');
  });
}

module.exports = RetentionTasks;