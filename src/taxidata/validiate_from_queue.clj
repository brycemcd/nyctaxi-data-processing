(ns taxidata.validiate-from-queue
  "take messages off an input queue and check the record's validity.

  If valid, put it on a valid records queue. If not, put it on an invalid records
  queue.
  "
  (:require
    [kinsky.client      :as client]
    [taxidata.input-impl-file :refer [create-trip]]
    [taxidata.core :refer [validate-trip]]
    ))

(def inbound-event-topic
  "topic name to pull raw events. NOTE: this should be a config"

   "inbound-trip-events")

(def valid-event-topic
  "topic name to write valid events. NOTE: this should be a config"

  "valid-trip-events")

(def invalid-event-topic
  "topic name to write invalid events. NOTE: this should be a config"

  "invalid-trip-events")

(def producer-config
  "eden formatted config. Specs at http://kafka.apache.org/documentation.html#producerconfigs"
  {:bootstrap.servers "10.1.2.206:9092"})

(def row-counter
  (atom 0))

(def trip-set
  (atom #{}))

(defn write-result-record-to-queue
  "blocking write to a kafka queue. If valid, write to one queue. If not, write to the other"
  [trip topic]
  (let [p (client/producer producer-config
            (client/keyword-serializer)
            (client/edn-serializer))]
    (client/send! p {:topic topic
                     :key nil
                     :partition 0
                     :value trip} )))

(def rando
  (str (java.util.UUID/randomUUID)))
(def kafka-group-id
  (str "test-consumer-" rando))

(defn process-record
  [batch]
  (doseq [[topic records] (:by-topic batch)
          :when (= inbound-event-topic topic)]
    (doseq [record records]
      (let [validated-record (validate-trip (create-trip (:value record)))
            record-unique-str (hash (str (:vendor_id validated-record) (:pickup_longitude validated-record) (:pickup_latitude validated-record) (:dropoff_latitude validated-record) (:dropoff_longitude validated-record)))
            ]
        (swap! row-counter inc)
        (swap! trip-set conj record-unique-str)
        (println "validated " (deref row-counter) " :" record-unique-str " set count: " (count (deref trip-set)))
        (if (:valid validated-record)
          (write-result-record-to-queue validated-record valid-event-topic)
          (write-result-record-to-queue validated-record invalid-event-topic))))))

(def inbound-consumer
  (let [c (client/consumer {:bootstrap.servers "10.1.2.206:9092,10.1.2.208:9092,10.1.2.216:9092"
                            :group.id          kafka-group-id
                            :auto.offset.reset "earliest"
                            :enable.auto.commit true}
            (client/keyword-deserializer)
            (client/string-deserializer))]

    (client/subscribe! c inbound-event-topic)
    c ))

(defn read-from-inbound-queue
  []
  ;(let [c (client/consumer {:bootstrap.servers "10.1.2.206:9092,10.1.2.208:9092,10.1.2.216:9092"
  ;                          :group.id          kafka-group-id
  ;                          :auto.offset.reset "earliest"
  ;                          :enable.auto.commit true}
  ;          (client/keyword-deserializer)
  ;          (client/string-deserializer))]
  ;  (client/subscribe! c inbound-event-topic)
    (let [c inbound-consumer
          records (client/poll! c 1000)]
      (process-record records)
      (recur)))
