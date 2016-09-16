const moment = require('moment')
const _ = require('underscore')

module.exports = {
  sanitizeServiceBusError: sanitizeServiceBusError,
  getLockDurationInSeconds: getLockDurationInSeconds,
  durationToIsoString: durationToIsoString,
  validateSimpleMessage: validateSimpleMessage,
  validateFullMessage: validateFullMessage
}

/**
* Converts non-standard object returned by azure sdk to plain object.
*/
function sanitizeServiceBusError(err) {
  if(err === null) {
    return null
  } else {
    return {
      code: err.code,
      statusCode: err.statusCode,
      detail: err.detail
    }
  }
}

/**
* Extracts information about lock duration form queueInfo object
* and returns in an easy form (seconds).
*/
function getLockDurationInSeconds(queueInfo) {

  if(!queueInfo.LockDuration)
    throw 'can\'t find queue\'s property "LockDuration"'

  // parse ISO 8601 duration and get as seconds
  return moment.duration(queueInfo.LockDuration).asSeconds()
}

function durationToIsoString(seconds) {
  return moment.duration(seconds, 'seconds').toISOString()
}

function validateSimpleMessage(msg) {

  if(_.isNull(msg)) {
    throw 'Message can\' be null.'
  }

  if(!_.isObject(msg)) {
    throw 'Message must be an object.'
  }
}

function validateFullMessage(msg) {

  validateSimpleMessage(msg)

  if(!_.has(msg, 'body')) {
    throw 'Message must have "body" field'
  }

  if(!_.isString(msg.body) && !Buffer.isBuffer(msg.body)) {
    throw 'Message body must be a string or a buffer.'
  }
}