/**
 * name : kafka.js.
 * author : Aman Karki.
 * created-date : 02-Feb-2021.
 * Description : kafka health check.
*/

// Dependencies
// Uses `kafkajs` (broker is Kafka 4.0.0). Interface unchanged: health_check() -> Promise<boolean>.
const { Kafka, logLevel } = require("kafkajs");

/**
 * Check Kafka connectivity by connecting a short-lived producer.
 * @method
 * @name health_check
 * @returns {Promise<boolean>} - true when the broker connection succeeds, false otherwise.
 */

function health_check() {
    return new Promise( async (resolve, reject) => {

        try {

            const brokers = (process.env.KAFKA_URL || "")
                .split(",")
                .map(function (broker) { return broker.trim(); })
                .filter(Boolean);

            const kafka = new Kafka({
                clientId : "ml-survey-service-healthcheck",
                brokers : brokers,
                logLevel : logLevel.NOTHING
            });

            const producer = kafka.producer();

            await producer.connect();
            await producer.disconnect();

            return resolve(true);

        } catch (error) {
            return resolve(false);
        }
    })
}

module.exports = {
    health_check : health_check
}
