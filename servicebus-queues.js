var azure = require('azure-sb');
var servicebusCommon = require('./servicebus-common')

var sanitizeServiceBusError = servicebusCommon.sanitizeServiceBusError
var validateSimpleMessage = servicebusCommon.validateSimpleMessage
var validateFullMessage = servicebusCommon.validateFullMessage
var getLockDurationInSeconds = servicebusCommon.getLockDurationInSeconds
var durationToIsoString = servicebusCommon.durationToIsoString

var connectionStr = process.env.SERVICEBUS_CONN_STR

if(!connectionStr)
  throw 'Connection string not provided. Define SERVICEBUS_CONN_STR env variable.'

var sbs = azure.createServiceBusService(connectionStr)

module.exports = {
  createQueue: createQueue,
  createQueueIfNotExists: createQueueIfNotExists,
  deleteQueue: deleteQueue,
  getQueueInfo: getQueueInfo,
  sendBrokeredMessage: sendBrokeredMessage,
  sendMessage: sendMessage,
  lockBrokeredMessage: lockBrokeredMessage,
  lockMessage: lockMessage,
  tryLockBrokeredMessage: tryLockBrokeredMessage,
  tryLockMessage: tryLockMessage,
  renewLock: renewLock,
  deleteMessage: deleteMessage,
  purge: purge,
  getLockDurationInSeconds: getLockDurationInSeconds,
  durationToIsoString: durationToIsoString
}

/**
* see https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L571
*/
function createQueue(queueName, options, cb) {
  sbs.createQueue(queueName, options, function(err, result, response) {
    cb(sanitizeServiceBusError(err), result, response)
  })
}

/**
* https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L604
*/
function createQueueIfNotExists(queueName, options, cb) {
  console.log('create queue:', queueName)
  sbs.createQueueIfNotExists(queueName, options, function(err, result, response) {
    console.log('queue created:', queueName)
    cb(sanitizeServiceBusError(err), result, response)
  })
}

/**
* https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L651
*/
function deleteQueue(queueName, cb) {
  console.log('delete queue:', queueName)
  sbs.deleteQueue(queueName, function(err, res) {
    console.log('queue deleted:', queueName)
    cb(sanitizeServiceBusError(err), res)
  })
}

/**
* Obtains information about the queue.
*
* @param {string} queueName             Name of the queue.
* @param {Function(err, queueInfo)} cb  `error` will contain information if an
*                                       error occurs
*                                       `queueInfo` queue properties
* @return {undefined}
*/
function getQueueInfo(queueName, cb) {
  sbs.getQueue(queueName, function(err, res) {
    cb(sanitizeServiceBusError(err), res)
  })
}

/**
* Sends a message created from an object (via JSON.stringify).
* If you want to set message properties use sendFullMessage
*
* @param {string} queueName   Name of the queue.
* @param {object} message     Object to be send as a JSON. If a message
*                             is not en object, exception is thrown.
* @param {Function(err)} cb   `err` will contain information if an
*                             error occurs
* @return {undefined}
*/
function sendMessage(queueName, message, cb) {

  validateSimpleMessage(message)

  var msgObj = {
    body: JSON.stringify(message, null, '  ')
  }

  sendBrokeredMessage(queueName, msgObj, function(err, response) {
    cb(sanitizeServiceBusError(err))
  })
}

/**
* Sends message which is an object in format of BrokeredMessage, described here:
* https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L417
* Message must be an object and contain 'body' field which is a string or Buffer
* If message isn't object with 'body' field, exception is thrown.
*
* @param {string} queueName   Name of the queue.
* @param {object} message     Object in for of BrokeredMessage.
* @param {Function(err)} cb   `err` will contain information if an error
*                             occurs
* @return {undefined}
*/
function sendBrokeredMessage(queueName, message, cb) {

  validateFullMessage(message)

  sbs.sendQueueMessage(queueName, message, function(err) {
    cb(sanitizeServiceBusError(err))
  });
}

/**
* Receives message (only body is returned via cb) in a peek-lock mode.
* If queue is empty, this method internally queries the queue until a message appears
* To lock full BrokeredMessage, use lockBrokeredMessage
*
* @param {string} queueName                    Name of the queue.
* @param {Function(err, msg, msgHandler)} cb   `err` will contain information
*                                              if an error occurs
*                                              `msg` contains body of the
*                                              message
*                                              `msgHandler` can be used to
*                                              delete message or renew lock
* @return {undefined}
*/
function lockMessage(queueName, cb) {

  lockBrokeredMessage(queueName, function(err, fullMsg, msgHandler) {

    if(!err) {

      var msg = JSON.parse(fullMsg.body)

      cb(err, msg, msgHandler)
    } else {
      cb(sanitizeServiceBusError(err))
    }
  });
}

/**
* Receives full message in a peek-lock mode. If queue is empty, this method internally
* queries the queue until a message appears
*
* @param {string} queueName                    Name of the queue.
* @param {Function(err, msg, msgHandler)} cb   `err` will contain information
*                                              if an error occurs
*                                              `msg` contains full message
*                                              `msgHandler` can be used to
*                                              delete message or renew lock
* @return {undefined}
*/
function lockBrokeredMessage(queueName, cb) {

  var options = {
    isPeekLock: true,
    timeoutIntervalInS: 60
  }

  function receive() {

    sbs.receiveQueueMessage(queueName, options, function(err, fullMsg) {

      if(!err) {
        cb(err, fullMsg, fullMsg.location)
      } else {
        if(err === 'No messages to receive') {
          receive()
        } else {
          cb(sanitizeServiceBusError(err))
        }
      }
    });
  }

  receive()
}


/**
* Receives message (only body is returned via cb) in a peek-lock mode.
* If queue is empty and given timeout is exceeded no error is issued,
* msg and msgHandler are null
*
* @param {string} queueName                    Name of the queue.
* @param {int}                                 The timeout interval, in seconds,
*                                              to use for the request.
* @param {Function(err, msg, msgHandler)} cb   `err` will contain information
*                                              if an error occurs
*                                              `msg` contains body of the
*                                              message
*                                              `msgHandler` can be used to
*                                              delete message or renew lock
* @return {undefined}
*/
function tryLockMessage(queueName, timeoutInSeconds, cb) {

  tryLockBrokeredMessage(queueName, timeoutInSeconds,
      function(err, fullMsg, msgHandler) {

    if(!err && fullMsg) {

      var msg = JSON.parse(fullMsg.body)
      cb(err, msg, msgHandler)

    } else {
      cb(sanitizeServiceBusError(err), null, null)
    }
  });
}


/**
* Receives full message in a peek-lock mode. If given timeout is exceeded,
* no error is returned, msg and handler are null.
*
* @param {string} queueName                    Name of the queue.
* @param {int}                                 The timeout interval, in seconds,
*                                              to use for the request.
* @param {Function(err, msg, msgHandler)} cb   `err` will contain information
*                                              if an error occurs
*                                              `msg` contains full message
*                                              `msgHandler` can be used to
*                                              delete message or renew lock
* @return {undefined}
*/
function tryLockBrokeredMessage(queueName, timeoutInSeconds, cb) {

  var options = {
    isPeekLock: true,
    timeoutIntervalInS: timeoutInSeconds
  }

  sbs.receiveQueueMessage(queueName, options, function(err, fullMsg) {

    if(!err) {
      cb(err, fullMsg, fullMsg.location)
    } else {
      if(err === 'No messages to receive') {
        cb(null, null, null)
      } else {
        cb(sanitizeServiceBusError(err))
      }
    }
  });
}

/**
* Deletes locked message.
*
* @param {string} msgHandler  Handler returned by lockMessage functions
* @param {Function(err)} cb   `err` will contain information
*                             if an error occurs

* @return {undefined}
*/
function deleteMessage(msgHandler, cb) {
  sbs.deleteMessage(msgHandler, function(err) {
    cb(sanitizeServiceBusError(err))
  })
}

/**
* Renews lock of formerly locked message. It's good idea to renew lock
* every lockDuration/2. See getLockDurationInSeconds().
*
* @param {string} msgHandler  Handler returned by lockMessage functions
* @param {Function(err)} cb   `err` will contain information
*                             if an error occurs

* @return {undefined}
*/
function renewLock(msgHandler, cb) {
  sbs.renewLockForMessage(msgHandler, function(err) {
    cb(sanitizeServiceBusError(err))
  })
}

/**
* For test only - shouldn't be used in production code!
* Removes all messages from the queue.
*/
function purge(queueName, cb) {

  function purgeInternal() {

    var options = {
      isPeekLock: false,
      timeoutIntervalInS: 0
    }

    sbs.receiveQueueMessage(queueName, options, function(err, response) {
      if(!err) {
        setImmediate(purgeInternal)
      } else if (err === 'No messages to receive') {
        cb(null)
      } else {
        //cb(sanitizeServiceBusError(err))
        cb(err, response)
      }
    });
  }

  purgeInternal()
}