(ns taxidata.validiate-from-queue
  "take messages off an input queue and check the record's validity.

  If valid, put it on a valid records queue. If not, put it on an invalid records
  queue.
  "
  (:require
    [taxidata.util-queue :as q]
    [kinsky.client :as client]
    [taxidata.input-impl-file :refer [create-trip]]
    [taxidata.core :refer [validate-trip]]
    [taxidata.input-impl-file :refer [is-file-header]]
    )
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
    org.apache.kafka.common.serialization.Deserializer
    org.apache.kafka.common.serialization.Serializer
    org.apache.kafka.common.serialization.StringDeserializer
    org.apache.kafka.common.serialization.StringSerializer ))

(def invalid-event-topic
  "topic name to write invalid events. NOTE: this should be a config"

  "invalid-trip-events")

(def valid-event-topic
  "topic name to write invalid events. NOTE: this should be a config"

  "valid-trip-events")

(def inbound-trip-event-topic "inbound-trip-events")

(defn write-to-validation-queue
  [validated-record]
  (let [valid? (:valid validated-record)
        queue-topic      (if valid? valid-event-topic invalid-event-topic)
       ]
    (print (if valid? "." "x"))
    (q/write-message-to-queue q/producer queue-topic validated-record)))

(defn make-trip-from-msg
  [msg]
  (create-trip msg))


(defn- get-value-from-consumer-record
  [rec]
  (clojure.string/trim-newline (.value rec)))


(defn create-validated-trip-from-raw-event
  ""
  [rec]
  (-> rec
    make-trip-from-msg
    validate-trip))

; copy/paste from https://techblog.roomkey.com/posts/clojure-kafka.html
(def c-cfg
  {"bootstrap.servers" "10.1.2.206:9092"
   "group.id" (str (java.util.UUID/randomUUID))
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false" ; FIXME: return to true
   "key.deserializer" StringDeserializer
   "value.deserializer" StringDeserializer})

(def consumer-raw (doto (KafkaConsumer. c-cfg)
                    (.subscribe [inbound-trip-event-topic])))


(defn read-trip-events-from-inbound-queue
  "Read batches of messages of the inbound queue and return a sequence of
  individual events to the caller"
  []
  (for [record (.records (.poll consumer-raw 100) inbound-trip-event-topic)]
        record))

(def get-trips-from-inbound-queue
  "Does just what it says. Higher order f(x) to compose a lot of smaller f(x)s"
  (map create-validated-trip-from-raw-event
    (remove is-file-header
      (map get-value-from-consumer-record (read-trip-events-from-inbound-queue)))))

(defn read-raw-trip-events-validate-and-write-to-validation-queue
  "Read a batch of trips, validate the batch, write them to the valid/invalid queue"
  ([]
  (map write-to-validation-queue get-trips-from-inbound-queue)))
