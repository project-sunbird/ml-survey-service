/**
 * name         : kafkaConfig.js
 * Description  : Kafka producer and rating-queue consumer setup.
 *
 * Uses `kafkajs` (the deployed broker is Kafka 4.0.0, which the previous
 * `kafka-node` client cannot connect to). The exported interface is unchanged,
 * so callers keep using:
 *   kafkaClient.kafkaProducer.send([{ topic, messages }], (err, data) => ...)
 * where, on success, `data` is `{ [topic]: [number] }` (callers check
 * `data[topic][0] > 0`).
 */

// dependencies
const { Kafka, logLevel } = require("kafkajs");

/**
 * Create the Kafka producer and, when configured, the rating-queue consumer.
 * @method
 * @name connect
 * @returns {Object} - { kafkaProducer, kafkaClient, kafkaKeyedMessage }.
 */

var connect = function () {

  const brokers = (process.env.KAFKA_URL || "")
    .split(",")
    .map(function (broker) { return broker.trim(); })
    .filter(Boolean);

  const kafka = new Kafka({
    clientId: "ml-survey-service",
    brokers: brokers,
    logLevel: logLevel.ERROR
  });

  const kafkajsProducer = kafka.producer();

  kafkajsProducer.connect()
    .then(function () {
      console.log("Connected to Kafka");
    })
    .catch(function (error) {
      console.error("kafka producer creation error!", error && error.message);
    });

  /**
   * Producer wrapper that preserves the kafka-node call signature.
   * @method
   * @name send
   * @param {Array} payload - [{ topic, messages }]; messages is a string or string[].
   * @param {Function} callback - (err, data); on success data is { [topic]: [number] }.
   */

  const producer = {
    send: function (payload, callback) {
      (async function () {
        try {
          const entry = payload[0];
          const topic = entry.topic;

          const rawMessages = Array.isArray(entry.messages) ? entry.messages : [entry.messages];
          const messages = rawMessages.map(function (message) {
            return { value: (typeof message === "string") ? message : JSON.stringify(message) };
          });

          const result = await kafkajsProducer.send({ topic: topic, messages: messages });

          // Map kafkajs metadata to the kafka-node shape { [topic]: [offset] }.
          // baseOffset can legitimately be "0" (first record), so report a
          // marker of 1 in that case to keep callers' `data[topic][0] > 0` true.
          const metadata = (Array.isArray(result) && result.length) ? result[0] : {};
          const baseOffset = (metadata.baseOffset !== undefined && metadata.baseOffset !== null)
            ? Number(metadata.baseOffset)
            : 0;
          const deliveredMarker = (baseOffset > 0) ? baseOffset : 1;

          const data = {};
          data[topic] = [deliveredMarker];

          return callback(null, data);
        } catch (error) {
          return callback(error);
        }
      })();
    }
  };

  if (process.env.SUBMISSION_RATING_QUEUE_TOPIC && process.env.SUBMISSION_RATING_QUEUE_TOPIC != "OFF") {

    const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID });

    (async function () {
      try {
        await consumer.connect();

        await consumer.subscribe({
          topic: process.env.SUBMISSION_RATING_QUEUE_TOPIC,
          fromBeginning: false
        });

        await consumer.run({
          autoCommit: true,
          eachMessage: async function ({ topic, partition, message }) {

            console.log("-------Kafka consumer log starts here------------------");
            console.log("Topic Name: ", process.env.SUBMISSION_RATING_QUEUE_TOPIC);
            console.log("Message: ", JSON.stringify(message));
            console.log("-------Kafka consumer log ends here------------------");

            // `submissionRatingQueueConsumer` is a runtime global registered by
            // config/globalVariable.js. It reads `message.value`; kafkajs gives
            // a Buffer, so pass a { value } object with a string value.
            submissionRatingQueueConsumer.messageReceived({
              value: message.value ? message.value.toString() : message.value
            });
          }
        });
      } catch (error) {
        submissionRatingQueueConsumer.errorTriggered(error);
      }
    })();

  }

  return {
    kafkaProducer: producer,
    kafkaClient: kafka,
    kafkaKeyedMessage: undefined
  };

};

module.exports = connect;
