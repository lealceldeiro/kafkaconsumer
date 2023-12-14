# Example Java application: Apache Kafka Consumer

An example of a consumer Java application that consumes events from an Apache Kafka broker

## Requirements

- Have installed Apache Kafka
  (see [How to run it sing Docker](https://lealceldeiro.github.io/gems/KafkaTheDefinitiveGuide/Chapter2/))
- Create a topic in the broker. i.e.: `topic1`
- Optionally, start a producer (i.e.: https://github.com/lealceldeiro/kafkaproducer)

## Build and run

- Run `./mvnw compile exec:java -Dexec.mainClass="com.kafkaexamples.kafkaconsumer.KafkaConsumerMain" -Dexec.args="localhost:9094 1 topic1"`
- Check the consoles as messages (consumed from the broker) start to appear on the console
- Type `/exit` and press enter to stop the program (it may take a while to completely shut down -- wait patiently)

> Note: replace the provided arguments with the correct values for your Kafka cluster. In the previous example:
> 
> - `localhost:9094` is the broker url
> - `1` is the number of consumers to be started
> - `topic1` is the topic name to subscribe to
