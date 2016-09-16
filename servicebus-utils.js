const queues = require('./servicebus-queues')
const queuesCommon = require('./servicebus-common')

module.exports.createQueueServiceBus = queues.createQueueServiceBus

module.exports.getLockDurationInSeconds = queuesCommon.getLockDurationInSeconds
module.exports.durationToIsoString = queuesCommon.durationToIsoString