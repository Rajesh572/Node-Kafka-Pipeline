var fs = require('fs');
var superagent = require('superagent');
var keys = require('./index')

var kafka = require('kafka-node');
var Client = kafka.KafkaClient;
var client = new Client('localhost:2181');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var producer = new Producer(client);
var options = {
  autoCommit: true,
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024,
  encoding: 'utf8'
};
var topics = [{
  topic: 'telemetrytest1'
}
];
var consumer = new Consumer(client, topics, options);
console.log('pipeline started');
consumer.on('message', (message) => {
  console.log('message received');
  try {
    let eventsArray = JSON.parse(JSON.parse(JSON.parse(message.value).message).message).events;

    eventsArray.forEach(element => {
      let url1 = 'https://camino.stackroute.com/api/user/v1/read/';
      let eventObject = element;
      let channel = eventObject.context.channel;
      let userId = eventObject.actor.id;
      let dateTime = new Date(eventObject.ets);
      let hours = dateTime.getHours();
      let minutes = dateTime.getMinutes();
      eventObject.hours = hours;
      eventObject.minutes = minutes;

      if (hours >= 5 && hours < 12) {
        eventObject.interval = "Morning";
      }
      if (hours >= 12 && hours < 15) {
        eventObject.interval = "Noon"
      }
      if (hours >= 15 && hours <= 19) {
        eventObject.interval = "Evening"
      }
      if ((hours >= 19 && hours <= 23) || (hours >= 0 && hours < 5)) {
        eventObject.interval = "Night"
      }

      superagent
        .get(url1 + userId)
        .set('Authorization', keys.api_token)
        .set('Content-Type', 'application/json')
        .set('x-authenticated-user-token', keys.x_auth_token)
        .end((err, res) => {
          if (err) {
            eventObject.userName = "Anonymous"
          }
          if (res) {

            try {
              data = JSON.parse(res.text);
              username = data.result.response.userName;
              eventObject.userName = username;
            }

            catch (e) {
              eventObject.userName = "Anonymous"

            }
          }
          eventObject.channelName = "niit-Channel"

          let object = JSON.stringify(eventObject);
          let payloads = [{ topic: "eventslogstest1", messages: object }];
          producer.send(payloads, function (err, data) {
            if (err) console.log('error occured while writing to kafka');
            if (data) console.log('successfully written to kafka', data);
          });
        });
    });
  }
  catch (err) { }
})