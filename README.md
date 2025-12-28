# kafka-app

Simple KafkaJS example app with admin/producer/consumer scripts.

Running

1. Install dependencies:

	npm install

2. (Optional) Configure broker(s) and client id via environment vars:

	export KAFKA_BROKERS="localhost:9092"
	export KAFKA_CLIENT_ID="my-client"

3. Create topic:

	npm run admin

4. Start consumer (long-running):

	npm run consumer

5. Produce a test message:

	npm run producer

Notes

- Update `client.js` to point to your Kafka broker addresses if not using environment variables.
- If your Kafka cluster requires authentication (SASL/SSL), configure that in `client.js`.
