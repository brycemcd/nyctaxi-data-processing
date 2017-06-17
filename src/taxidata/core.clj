(ns taxidata.core
  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]
            [kixi.stats.core :as stats]
            ))

(declare mapify)

;NOTE: it munges the trip_type keys into any order

(def filename100 "data/trip_2016_06-100.csv")
(def filenameAll "data/yellow_tripdata_2016-06.csv")

;(def mapfied-trips (mapify (read_trip_file (slurp filename))))
;(def mapified-trips (with-open [r (clojure.java.io/input-stream filename)]
                    ;(loop [c (.read r)]
                      ;(if (not= c -1)
                        ;(do
                          ;(print (char c))
                          ;(recur (.read r)))))))
(def to_int #(Integer. %))
(def to_dec #(Double. %))
(def to_dttm #(dttm_f/parse (dttm_f/formatters :mysql) %)) ; NOTE: mysql formatter is YYYYMMDD HH:mm::ss

; the functions that casts these values as the proper type
(def trip_types [[:vendor_id to_int]
                [:tpep_pickup_datetime to_dttm]
                [:tpep_dropoff_datetime to_dttm]
                [:passenger_count to_int]
                [:trip_distance to_dec]
                [:pickup_longitude to_dec]
                [:pickup_latitude to_dec]
                [:ratecode_id to_int]
                [:store_and_fwd_flag identity]
                [:dropoff_longitude to_dec]
                [:dropoff_latitude to_dec]
                [:payment_type identity]
                [:fare_amount to_dec]
                [:extra to_dec]
                [:mta_tax to_dec]
                [:tip_amount to_dec]
                [:tolls_amount to_dec]
                [:improvement_surcharge to_dec]
                [:total_amount to_dec]])

(def trip_header (map first trip_types))

(defn read_trip_file
  "Reads a trip file and parses it"
  [rows]
  (map #(clojure.string/split % #",")
       (clojure.string/split rows #"\r\n")))

(defn convert
  "converts a string into the right value of the trip data"
  [trip_key value]
  ((get (into {} trip_types) trip_key) value))

(defn mapify
  [rows]
  (map (fn [unmapped-row]
         (reduce (fn [row-map [trip_key value]]
                   (assoc row-map trip_key (convert trip_key value)))
                 {}
                 (map vector trip_header unmapped-row)))
       rows))


; BASIC MATHS
; The calc-functions here are designed to provide easily accessible calculations
; useful for verifying and auditing rows later

; TODO: remove the column arguments and just calc a sum on the collection
(defn calc-sum
  "Take a file and mapify"
  ([]
   (reduce + (map :tip_amount mapfied-trips)))
  ([column]
   (reduce + (map column mapfied-trips)))
  ([column n]
   (reduce + (map column (take n mapfied-trips)))))

; TODO: take into account missing fields
(defn calc-count
  ""
  ([n]
   (count (take n mapfied-trips)))
  ([]
   (count mapfied-trips)))

(defn calc-mean
  "calculate a simple mean of a collection"
  [coll]
  (let [sum (apply + coll)
        cnt (count coll)]
    (if (pos? cnt)
      (/ sum cnt)
      0)))
  ;(->> mapfied-trips (transduce (map column) stats/mean)))

(defn calc-stddev
  "calculate the stddev of a collection"
  [coll]
  (let [avg (calc-mean coll)
        squares (for [x coll]
                  (let [x-avg (- x avg)]
                    (* x-avg x-avg)))
        total (count coll)]
    (-> (/ (apply + squares)
           (- total 1))
        (Math/sqrt))))

(defn extreme?
  "determines if amount is unreasonable to include in analysis. For now extreme
  is defined as 3 times the standard deviation"

  [column value]
  (> value (* 3 (calc-stddev column)))) ; TODO: magic number 3 is conventional but not easy to change in this code

(def extreme-tip-amount? #(extreme? :tip_amount %))


(defn mapify-row
  [rows]
  (map #(clojure.string/split % #",") rows))

(defn mapified-trips
  [fname]
  (let [x 1]
    (with-open [rdr (clojure.java.io/reader fname)]
      (doseq [line (line-seq rdr)]
        (+ x (to_int (first (mapify-row line))))))
    (println x)))

(defn lazy-file-lines
  [file]
  (letfn [(helper [rdr]
            (lazy-seq
              (if-let [line (.readLine rdr)]
                (cons line (helper rdr))
                (do
                  (.close rdr)
                  nil))))]
    (helper (clojure.java.io/reader file))))

; call with (reduce + (map to_int (map first (mapify-row (lazy-file-lines filename)))))
