(ns taxidata.input-impl-kafka
  "Receives input from a file and writes records to be processed to a Kafka queue"
  (:require
    [kinsky.client      :as client]
    [taxidata.input-impl-file :refer [is-file-header]]))

(def producer-config
  "eden formatted config. Specs at http://kafka.apache.org/documentation.html#producerconfigs"
  {:bootstrap.servers "10.1.2.206:9092,10.1.2.208:9092,10.1.2.216:9092"})

(def queue-topic
  "Topic name to write events. NOTE: this should be a config"

  "inbound-trip-events")

(def row-counter
  (atom 0))

(def inbound-queue-inst
  "Connection to a kafka queue for writing raw records. Single instantiation"
  (client/producer producer-config
                   (client/keyword-serializer)
                   (client/string-serializer)))

(defn write-to-inbound-queue
  "blocking write to a kafka queue. Expects a single line from a taxi export as input"
  [record]
  (swap! row-counter inc)
  (if (= 0 (mod (deref row-counter) 100000))
    (println (str "written: " (deref row-counter))))

  (client/send! inbound-queue-inst queue-topic :v1 record))

(defn main
  "Given a filename, parse the file and add event records to a Kafka queue"
  [filename]
  (with-open [rdr (clojure.java.io/reader filename)]
    (doseq [line (line-seq rdr)]
      (if-not (is-file-header line)
        (write-to-inbound-queue line)))))
