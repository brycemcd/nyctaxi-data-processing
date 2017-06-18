(ns taxidata.core
  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]
            [kixi.stats.core :as stats]
            ))
(declare mapify)

;NOTE: it munges the trip_type keys into any order

(def filename100 "data/trip_2016_06-100.csv")
(def filenameAll "data/yellow_tripdata_2016-06.csv")

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


(defn convert-value
  "converts a string into the right value of the trip data"
  [trip_key value]
  ((get (into {} trip_types) trip_key) value))

(defn convert-row
  "Takes in a raw imported row of trip data and returns the row with the values
  coerced into the proper data type"
  [row]
  (reduce (fn [row-map [trip-key value]]
            (assoc row-map trip-key (convert-value trip-key value)))
          {}
          (into [] row)))

; BASIC MATHS
; The calc-functions here are designed to provide easily accessible calculations
; useful for verifying and auditing rows later

; TODO: take into account missing fields

; https://github.com/clojure-cookbook/clojure-cookbook/blob/master/01_primitive-data/1-20_simple-statistics.asciidoc
(defn calc-mean
  "calculate a simple mean of a collection"
  [coll]
  (let [sum (apply + coll)
        cnt (count coll)]
    (if (pos? cnt)
      (/ sum cnt)
      0)))

; https://github.com/clojure-cookbook/clojure-cookbook/blob/master/01_primitive-data/1-20_simple-statistics.asciidoc
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

; IMPORT AND PREPARE DATA
(defn split-row
  "splits a csv row on the comma"
  [row]
  (clojure.string/split row #","))

(defn import-file
  "imports a file and converts each row into a map with properly typed values.
  Returns a lazy sequence of file rows"
  [file]
  (letfn [(helper [rdr]
            (lazy-seq
              (if-let [line (.readLine rdr)]
                (cons (convert-row (zipmap trip_header (split-row line))) (helper rdr))
                (do
                  (.close rdr)
                  nil))))]
    (helper (clojure.java.io/reader file))))

; VERIFY ROWS
; NOTE: the approach for verifying is to take the raw row and pass over a series
; of filters that will detect anomalous values. If a row is found to have values
; within our tolerances, then a {:valid true} map is appended to the row.
; {:verified true} is added to the row after all validations step complete

(defn extreme-numeric?
  "determines if value is reasonable to include in analysis. For now extreme
  is defined as 3 times the standard deviation"
  ; TODO: magic number 3 is conventional with respect to a normal distribution
  ; but non verification has been done to confirm the values in this data are
  ; normal. Be sure to update the README if validity criteria change
  [value mean stddev]
  (> value (* 3 (+ mean stddev))))

(def not-extreme-numeric? (complement extreme-numeric?))

; FIXME: I don't quite have this right. I want to be able to run validity
; functions indempotently and NOT overwrite a :valid false with a :valid true
; and cause problems later when an invalid row is overwritten with false

; needs to contain key && mapkey && have key be false to skip, else process
; TODO: refactor this. I'm incredibly distracted (penny is singing at the top
; of her lungs in the tub) and trying to just get this function correct
(defn add-valid-for-numeric!
  "adds {:valid false} for a numeric key iff validity check fails"
  [mapkey row verified-fx?]
  (if (> (mapkey row) 20)
    (assoc row :valid false)
    row))

(defn audit-numeric-column
  [imported-rows column]
  (let [mean   (calc-mean (map column imported-rows))
        stddev (calc-stddev (map column imported-rows))
        passesaudit? #(not-extreme-numeric? % mean stddev)
        ]
    (println (str "calling on " column))
    (map (fn [row] (add-valid-for-numeric! column row passesaudit?)) imported-rows)))

(defn audit-numerics
  "Takes in a lazy sequence of rows and verifies values are not extreme"
  ; TODO: needs refactoring. I'm stumbling through this. Lots of debug statements
  ; start with "tip_amount" column and then abstract to all"
  [imported-rows]
  ;(loop [mapkeys (seq )
         ;rows (take 10 imported-rows)]
   (loop [mapkeys [:tip_amount :trip_distance :fare_amount :extra :mta_tax :tip_amount :tolls_amount :total_amount]
          rows imported-rows]

    (if (first mapkeys)
      (recur (rest mapkeys) (audit-numeric-column rows (first mapkeys)) )
      rows)))

      ;(recur( (rest mapkeys) (take 10 imported-rows) )) ;(map #(audit-numeric-column % (first mapkeys)) rows))))
; SCRATCHPAD
; call with (reduce + (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (calc-stddev (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (def first10 (take 10 (import-file filename100)))
; (audit-rows first10)