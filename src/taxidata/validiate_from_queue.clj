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
  "topic name to write invalid events. NOTE: this should be a config"

  "invalid-trip-events")

(def valid-event-topic
  "topic name to write invalid events. NOTE: this should be a config"

  "valid-trip-events")

(def inbound-trip-event-topic
  "topic name from which to get raw taxi events NOTE: this should be a config"

  "inbound-trip-events")

(defn stringify-joda-dttm
  "converts a joda dttm into a string I want to write to a queue"
  [dttm]
  (str (.toDateTimeISO dttm)))

(defn json-serialize-trip
  "converts trip record into something that can be json serialized to the queue

  TODO: this is not the right place for this method"
  [trip-record]
  ; TODO: I should be able to get dttm keys from a definition of the record
  (let [dttm-values [:tpep_pickup_datetime :tpep_dropoff_datetime]]
    (reduce #(update %1 %2 stringify-joda-dttm) trip-record dttm-values)))

(defn write-to-validation-queue
  "Take a validated trip and writes it to the appropriate queue"
  [validated-record]
  (let [valid? (:valid validated-record)
        queue-topic      (if valid? valid-event-topic invalid-event-topic)
        serializable-record (json-serialize-trip validated-record)
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
  "Returns just the 'value' key from the Kafka ConsumerRecord instance"
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
  (q/consumer-generic q/consumer-generic-cfg inbound-trip-event-topic))

(defn read-batch-from-inbound-queue
  "Read batches of messages off the inbound queue and return a sequence of
  individual events to the caller"
  []
  (q/read-from-inbound-queue inbound-event-consumer inbound-trip-event-topic))

(defn get-trips-from-inbound-queue
  "Reads events and parses them into trips in a batch. Higher order f(x) to
  compose a lot of smaller f(x)s"
  []
  (map create-validated-trip-from-raw-event
    (remove is-file-header
      (map get-value-from-consumer-record (read-batch-from-inbound-queue)))))

(defn process-events-to-validation-queues
  "Read a batch of trips, validate the batch, write them to the valid/invalid queue"
  ([]
  (loop []
    ; the map statement does not realize the lazy seq created. Doall forces it
    (doall (map write-to-validation-queue (get-trips-from-inbound-queue)))
    (recur))))
