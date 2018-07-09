(ns taxidata.util-queue
  (:require
    [kinsky.client :as client]
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

(def inbound-event-topic
  "topic name to pull raw events. NOTE: this should be a config"

   "inbound-trip-events")

(def producer-config
  "eden formatted config. Specs at http://kafka.apache.org/documentation.html#producerconfigs"
  {:bootstrap.servers "10.1.2.206:9092"})

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
  ""
  [config topic]
  (doto (KafkaConsumer. config)
    (.subscribe [topic])))

(def consumer-generic-cfg
  ""
  {"bootstrap.servers" "10.1.2.206:9092,10.1.2.208:9092,10.1.2.216:9092"
   "group.id" (str (java.util.UUID/randomUUID))
   "auto.offset.reset" "earliest"
   "enable.auto.commit" false ; FIXME: return to true
   "key.deserializer" StringDeserializer
   "value.deserializer" StringDeserializer})

(defn read-from-inbound-queue
  "Read batches of messages off the inbound queue and return a sequence of
  individual events to the caller"
  [consumer topic]
  (for [record (.records (.poll consumer 100) topic)]
        record))
