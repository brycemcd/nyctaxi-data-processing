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
    ))

(def invalid-event-topic
  "topic name to write invalid events"
  (:invalid-event-topic q/config))

(def valid-event-topic
  "topic name to write valid events"
  (:valid-event-topic q/config))

(def inbound-trip-event-topic
  "topic name from which to get raw taxi events"
  (:inbound-event-topic q/config))

(defn stringify-joda-dttm
  "converts a joda dttm into a string I want to write to a queue"
  [dttm]
  (str (.toDateTimeISO dttm)))

(defn json-serialize-trip-dttms
  "converts trip record dttms into something that can be json serialized to the queue

  TODO: this is not the right place for this method"
  [trip-record]
  ; TODO: I should be able to get dttm keys from a definition of the record
  (let [dttm-values [:tpep_pickup_datetime :tpep_dropoff_datetime]]
    (reduce #(update %1 %2 stringify-joda-dttm) trip-record dttm-values)))

(defn write-to-validation-queue
  "Take a validated trip and writes it to the valid/invalid queue"
  [validated-record]
  (let [valid? (:valid validated-record)
        queue-topic      (if valid? valid-event-topic invalid-event-topic)
        serializable-record (json-serialize-trip-dttms validated-record)
       ]
    (print (if valid? "." "x"))
    (q/write-message-to-queue q/producer queue-topic serializable-record)))

(defn make-trip-from-event
  "Converts a comma separated string into a trip record

  TODO: should move this to a utility namespace. The file mechanisms could use
  this too."
  [msg]
  (create-trip msg))

(defn- get-value-from-consumer-record
  "Returns just the 'value' key from a Kafka ConsumerRecord instance"
  [rec]
  (clojure.string/trim-newline (.value rec)))

(defn create-validated-trip-from-raw-event
  "Routine to convert a raw event into a validated trip record"
  [event]
  (-> event
    make-trip-from-event
    validate-trip))

(def inbound-event-consumer
  "An instance of a consumer to read raw inbound events"
  (let [user-config     (q/config :inbound-event-consumer-map)
        consumer-config (merge q/consumer-generic-cfg
                               user-config
                               {"bootstrap.servers" (:kafka-hosts q/config)})]

    (q/consumer-generic consumer-config inbound-trip-event-topic)))

(defn read-batch-from-inbound-queue
  "Read batches of messages off the inbound queue and return a sequence of
  individual events to the caller"
  []
  (q/consume-topic inbound-event-consumer inbound-trip-event-topic))

(defn convert-batch-to-validated-trip-events
  "Reads events and parses them into trips in a batch. Higher order f(x) to
  compose a lot of smaller f(x)s"
  []
  (pmap create-validated-trip-from-raw-event
    (remove is-file-header
      (pmap get-value-from-consumer-record (read-batch-from-inbound-queue)))))

(defn process-events-to-validation-queues
  "Read a batch of trips, validate the batch, write them to the valid/invalid queue"
  ([]
  (loop []
    ; the map statement does not realize the lazy seq created. Doall forces it
    (doall
      (pmap write-to-validation-queue (convert-batch-to-validated-trip-events)))
    (recur))))
