# Reactive Pulsar adapter

Reactive Streams adapter for Apache Pulsar Java Client.
This uses Project Reactor as the Reactive Streams implementation.

## Getting it

**This library requires Java 8 or + to run**.

With Gradle:

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation "com.github.lhotari:reactive-pulsar-adapter:0.0.2"
}
```

With Maven:
```xml
<dependencies>
    <dependency>
        <groupId>com.github.lhotari</groupId>
        <artifactId>reactive-pulsar-adapter</artifactId> 
        <version>0.0.2</version>
    </dependency>
</dependencies>
```

## Usage

### Initializing the library

Using an existing PulsarClient instance:

```java
ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);
```

### Sending messages

```java
ReactiveMessageSender<String> messageSender = reactivePulsarClient
        .messageSender(Schema.STRING)
        .topic(topicName)
        .maxInflight(100)
        .create();
Mono<MessageId> messageId = messageSender
        .sendMessage(Mono.just(MessageSpec.<String>builder().value("Hello world!").build()));
// for demonstration
messageId.subscribe(System.out::println);
```

### Sending messages with cached producer

Add require dependency for cache implementation. 

With Gradle:
```groovy
dependencies {
    implementation "com.github.lhotari:reactive-pulsar-adapter:0.0.2"
    implementation "com.github.lhotari:reactive-pulsar-caffeine-producer-cache:0.0.2"
}
```

With Maven:
```xml
<dependencies>
    <dependency>
        <groupId>com.github.lhotari</groupId>
        <artifactId>reactive-pulsar-adapter</artifactId> 
        <version>0.0.2</version>
    </dependency>
    <dependency>
        <groupId>com.github.lhotari</groupId>
        <artifactId>reactive-pulsar-caffeine-producer-cache</artifactId>
        <version>0.0.2</version>
    </dependency>
</dependencies>
```

```java
CaffeineReactiveProducerCache producerCache = new CaffeineReactiveProducerCache();
ReactiveMessageSender<String> messageSender = reactivePulsarClient
        .messageSender(Schema.STRING)
        .cache(producerCache)
        .topic(topicName)
        .maxInflight(100)
        .create();
Mono<MessageId> messageId = messageSender
        .sendMessage(Mono.just(MessageSpec.<String>builder().value("Hello world!").build()));
// for demonstration
messageId.subscribe(System.out::println);
```

It is recommended to use a cached producer in most cases. The cache enables reusing the Pulsar Producer instance and related resources across multiple message sending calls.
This improves performance since a producer won't have to be created and closed before and after sending a message.

The adapter library implementation together with the cache implementation will also enable reactive backpressure for sending messages. The `maxInflight` setting will limit the number of messages that are pending from the client to the broker. The solution will limit reactive streams subscription requests to keep the number of pending messages under the defined limit. This limit is per-topic and impacts the local JVM only. 


### Reading messages

Reading all messages for a topic:
```java
    ReactiveMessageReader<String> messageReader =
            reactivePulsarClient.messageReader(Schema.STRING)
                    .topic(topicName)
                    .create();
    messageReader.readMessages()
            .map(Message::getValue)
            // for demonstration
            .subscribe(System.out::println);
```
By default, the stream will complete when end of the topic is reached. The end of the topic is detected with Pulsar Reader's `hasMessageAvailableAsync` method.


The ReactiveMessageReader doesn't support partitioned topics. It's possible to read the content of indidual partitions. Topic names for individual partitions can be discovered using the PulsarClient's `getPartitionsForTopic` method. The adapter library doesn't currently wrap that method.

#### Example: poll for 5 new messages until a timeout occurs 

With `.endOfStreamAction(EndOfStreamAction.POLL)` the Reader will poll for new messages when the reader reaches the end of the topic.

```java
    ReactiveMessageReader<String> messageReader =
            reactivePulsarClient.messageReader(Schema.STRING)
                    .topic(topicName)
                    .startAtSpec(StartAtSpec.LATEST)
                    .endOfStreamAction(EndOfStreamAction.POLL)
                    .create();
    messageReader.readMessages().take(5)
            .timeout(Duration.ofSeconds(5))
            // for demonstration
            .subscribe(System.out::println);

```

### Consuming messages

```java
    ReactiveMessageConsumer<String> messageConsumer =
            reactivePulsarClient.messageConsumer(Schema.STRING)
                    .topic(topicName)
                    .consumerConfigurer(consumerBuilder -> consumerBuilder.subscriptionName("sub"))
                    .create();
    messageConsumer.consumeMessages()
            .map(consumedMessage -> {
                consumedMessage.acknowledge();
                return consumedMessage.getMessage().getValue();
            })
            .timeout(Duration.ofSeconds(2), Mono.empty())
            // for demonstration
            .subscribe(System.out::println);
```

### Consuming messages using a message handler component with auto-acknowledgements

```java
ReactiveMessageHandler reactiveMessageHandler =
   reactivePulsarClient.messageHandler(Schema.STRING)
           .consumerConfigurer(consumerBuilder ->
                   consumerBuilder.subscriptionName("sub")
                           .topic(topicName))
           .messageHandler(message -> Mono.fromRunnable(() -> {
               System.out.println(message.getValue());
           }))
          .start();
// for demonstration
// the reactive message handler is running in the background, delay for 10 seconds
Thread.sleep(10000L);
// now stop the message handler component
reactiveMessageHandler.close();
```

## License

Reactive Pulsar adapter library is Open Source Software released under the [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0).


## How to Contribute

The library is Apache 2.0 licensed and accepts contributions via GitHub pull requests.

* Fork it
* Create your feature branch (`git checkout -b my-new-feature`)
* Commit your changes (`git commit -am 'Added some feature'`)
* Push to the branch (`git push origin my-new-feature`)
* Create new Pull Request

##  Bugs and Feature Requests

If you detect a bug or have a feature request or a good idea for Reactive Pulsar adapter, please [open a GitHub issue](https://github.com/lhotari/reactive-pulsar/issues/new) or ping one of the contributors on Twitter or on [Pulsar Slack](https://pulsar.apache.org/en/contact/).