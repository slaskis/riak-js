/**
 * Module dependencies.
 */
var HttpClient = require('./http-client');

/**
 * Obtains an instance of `HttpClient`.
 *
 * @param {Object|Meta} options [optional]
 * @return {HttpClient}
 * @api public
 */
exports.getClient = function(options){
  return new HttpClient(options);
}