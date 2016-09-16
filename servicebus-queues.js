const azure = require('azure-sb');
const servicebusCommon = require('./servicebus-common')

const sanitizeServiceBusError = servicebusCommon.sanitizeServiceBusError
const validateSimpleMessage = servicebusCommon.validateSimpleMessage
const validateFullMessage = servicebusCommon.validateFullMessage
const getLockDurationInSeconds = servicebusCommon.getLockDurationInSeconds
const durationToIsoString = servicebusCommon.durationToIsoString

// var connectionStr = process.env.SERVICEBUS_CONN_STR
// if(!connectionStr)
//   throw 'Connection string not provided. Define SERVICEBUS_CONN_STR env variable.'
// var sbs = azure.createServiceBusService(connectionStr)

module.exports = {
  createQueueServiceBus: (connectionString) => {
    return new QueueServiceBus(connectionString)
  }
}

class QueueServiceBus {

  constructor(connectionStr) {
    this._sbs = azure.createServiceBusService(connectionStr)
  }

  /**
  * see https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L571
  */
  createQueue(queueName, options, cb) {
    this._sbs.createQueue(queueName, options, (err, result, response) => {
      cb(sanitizeServiceBusError(err), result, response)
    })
  }

  /**
  * https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L604
  */
  createQueueIfNotExists(queueName, options, cb) {
    console.log('create queue:', queueName)
    this._sbs.createQueueIfNotExists(queueName, options, (err, result, response) => {
      console.log('queue created:', queueName)
      cb(sanitizeServiceBusError(err), result, response)
    })
  }

  /**
  * https://github.com/Azure/azure-sdk-for-node/blob/master/lib/services/serviceBus/lib/servicebusservice.js#L651
  */
  deleteQueue(queueName, cb) {
    console.log('delete queue:', queueName)
    this._sbs.deleteQueue(queueName, (err, res) => {
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
  getQueueInfo(queueName, cb) {
    this._sbs.getQueue(queueName, (err, res) => {
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
  sendMessage(queueName, message, cb) {

    validateSimpleMessage(message)

    const msgObj = {
      body: JSON.stringify(message, null, '  ')
    }

    this.sendBrokeredMessage(queueName, msgObj, (err, response) => {
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
  sendBrokeredMessage(queueName, message, cb) {

    validateFullMessage(message)

    this._sbs.sendQueueMessage(queueName, message, (err) => {
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
  lockMessage(queueName, cb) {

    this.lockBrokeredMessage(queueName, (err, fullMsg, msgHandler) => {

      if(!err) {

        const msg = JSON.parse(fullMsg.body)

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
  lockBrokeredMessage(queueName, cb) {

    const options = {
      isPeekLock: true,
      timeoutIntervalInS: 60
    }

    const receive = () => {

      this._sbs.receiveQueueMessage(queueName, options, (err, fullMsg) => {

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
  tryLockMessage(queueName, timeoutInSeconds, cb) {

    this.tryLockBrokeredMessage(queueName, timeoutInSeconds,
        (err, fullMsg, msgHandler) => {

      if(!err && fullMsg) {

        const msg = JSON.parse(fullMsg.body)
        cb(err, msg, msgHandler)

      } else {
        cb(sanitizeServiceBusError(err), null, null)
      }
    })
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
  tryLockBrokeredMessage(queueName, timeoutInSeconds, cb) {

    const options = {
      isPeekLock: true,
      timeoutIntervalInS: timeoutInSeconds
    }

    this._sbs.receiveQueueMessage(queueName, options, (err, fullMsg) => {

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
  deleteMessage(msgHandler, cb) {
    this._sbs.deleteMessage(msgHandler, (err) => {
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
  renewLock(msgHandler, cb) {
    this._sbs.renewLockForMessage(msgHandler, (err) => {
      cb(sanitizeServiceBusError(err))
    })
  }

  /**
  * For test only - shouldn't be used in production code!
  * Removes all messages from the queue.
  */
  purge(queueName, cb) {

    const purgeInternal = () => {

      const options = {
        isPeekLock: false,
        timeoutIntervalInS: 0
      }

      this._sbs.receiveQueueMessage(queueName, options, (err, response) => {
        if(!err) {
          setImmediate(purgeInternal)
        } else if (err === 'No messages to receive') {
          cb(null)
        } else {
          //cb(sanitizeServiceBusError(err))
          cb(err, response)
        }
      })
    }

    purgeInternal()
  }
}


