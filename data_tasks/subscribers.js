'use strict';

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
}

module.exports = SubscriberTasks;