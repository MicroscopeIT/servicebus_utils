// var azure = require('azure');
// var _ = require('underscore')

// console.log('initializing service bus');

// var sbs = azure.createServiceBusService()

// console.log('service bus ready');


// function sendMessageSimple(subscriptionName, message, properties, cb) {

//   if(_.isObject(message)) {
//     message = JSON.stringify(message, null, '  ');
//   }

//   sendMessage(queueName, message, cb);
// }


// function sendMessage(subscriptionName, message, cb) {

//   sbs.sendQueueMessage(queueName, message, function(error) {

//    // console.log('message:', message);

//     if(!error) {
//       // message sent
//       console.log('message sent!');
//     } else {
//       console.log('error!')
//     }

//     cb(error);
//   });
// }

// function purge(subscriptionName, cb) {

//   function purgeInternal() {

//     sbs.receiveQueueMessage(queueName, { isPeekLock: false }, function(err, receivedMessage) {
//       if(!err) {
//         //purgeInternal();
//         setImmediate(purgeInternal)
//       } else {
//         cb(err, receivedMessage)
//       }
//     });
//   }

//   purgeInternal()
// }

// function lockMessageSimple(subscriptionName, cb) {

//   lockMessage(queueName, function(err, receivedMessage) {

//     if(!err) {
      
//     }


//   });
// }

// function lockMessage(subscriptionName, cb) {

//   sbs.receiveQueueMessage(queueName, { isPeekLock: true }, function(err, receivedMessage) {
//     //  if(!err) {
//           // Message received and deleted

//           //console.log('message')
//           //console.log(util.inspect(receivedMessage, {depth: null, colors: true}))

//           cb(err, receivedMessage);

//           // sbs.renewLockForMessage(receivedMessage, function(err, res) {

//           //   console.log(err)
//           //   console.log(res)

//           // })

//       // } else {
//       //   console.log(err)
//       // }
//   });
// }

// function deleteMessage(subscriptionName, cb) {

// }

// function renewLock(subscriptionName, cb) {
  
// }