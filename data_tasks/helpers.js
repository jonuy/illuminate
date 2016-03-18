'use strict';

var fs = require('fs');

module.exports = {

  /**
   * Returns a date in yyyy-m-d format.
   *
   * @param date Date
   */
  formatDateForQuery: function(date) {
    return date.getFullYear() + '-'
      + (date.getMonth() + 1) + '-'
      + date.getDate();
  },

  /**
   * Writes data to a csv.
   *
   * @param labels Array Single array of labels for the first row of the csv
   * @param rows Array Double array of the data to write to the csv
   * @param filename String
   * @param done Callback called when file is written
   */
  writeToCsv: function(labels, rows, filename, done) {
    let csvString = "";

    for (let i = 0; i < labels.length; i++) {
      csvString += labels[i];

      if (i < labels.length - 1) {
        csvString += ',';
      }
    }

    csvString += '\n';
    for (let i = 0; i < rows.length; i++) {
      for (let j = 0; j < rows[i].length; j++) {
        csvString += rows[i][j];

        if (j < rows[i].length - 1) {
          csvString += ',';
        }
      }
      csvString += '\n';
    }

    fs.writeFile('./output/' + filename, csvString, function(err) {
      if (err) {
        return console.log(err);
      }

      console.log('Saved: ' + filename);

      if (typeof done === 'function') {
        done();
      }
    });
  }

};