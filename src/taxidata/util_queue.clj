(ns taxidata.util-queue
  (:require
    [kinsky.client :as client]
    [cprop.core :refer [load-config]]
    )
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
    org.apache.kafka.common.serialization.Deserializer
    org.apache.kafka.common.serialization.Serializer
    org.apache.kafka.common.serialization.StringDeserializer
    org.apache.kafka.common.serialization.StringSerializer ))

(def rando
  (str (java.util.UUID/randomUUID)))

(def kafka-group-id
  (str "test-consumer-" rando))

(def config
  "the edn file to parse for configuration"
  (load-config :file "queue-configuration.edn"))

(def producer-config
  "eden formatted config. Specs at http://kafka.apache.org/documentation.html#producerconfigs"
  {:bootstrap.servers (:kafka-hosts config)})

(def producer
  "blocking write to a kafka queue."
  (client/producer producer-config
    (client/keyword-serializer)
    (client/json-serializer)))

(defn write-message-to-queue
  "writes message to queue

  producer: instance of a kafka producer
  topic: kafka topic to publish
  key: message key to publish
  value: message to publish
  "
  ([producer topic key value]
    (client/send! producer topic key value))
  ([producer topic value]
    (client/send! producer topic nil value)))

(defn consumer-generic
  "A generic consumer configuration. Provide a config in order to pull batches
  of messages off a queue

  config: map providing KafkaConsumer configuration options (see Kafka java docs)
  topic: string name of topic to consume
  "
  [config topic]
  (doto (KafkaConsumer. config)
    (.subscribe [topic])))

(def consumer-generic-cfg
  "Generic config with sensible defaults"
  {"group.id" kafka-group-id
   "auto.offset.reset" "latest"
   "key.deserializer" StringDeserializer
   "value.deserializer" StringDeserializer})

(defn consume-topic
  "Read batches of messages off the inbound queue and return a sequence of
  individual events to the caller"
  [consumer topic]
  (for [record (.records (.poll consumer 100) topic)]
        record))
