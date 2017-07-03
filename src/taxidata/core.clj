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
(defn to_int
  [value]
  (Integer. value))

(defn to_dec
  [value]
  (Double. value))

(defn to_dttm
  [value]
  (dttm_f/parse (dttm_f/formatters :mysql) value)) ; NOTE: mysql formatter is YYYYMMDD HH:mm::ss

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
                [:payment_type to_int]
                [:fare_amount to_dec]
                [:extra to_dec]
                [:mta_tax to_dec]
                [:tip_amount to_dec]
                [:tolls_amount to_dec]
                [:improvement_surcharge to_dec]
                [:total_amount to_dec]])

(def trip_header (map first trip_types))



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
      0.0)))

(defn- x-avg-squared
  [x avg]
  (* (- x avg) (- x avg)))

(defn calc-stddev
  "calculate the stddev of a collection"
  ([coll]
   (calc-stddev coll (calc-mean coll)))
  ([coll avg]
   (let [squares (map #(x-avg-squared % avg) coll)
         cnt (count coll)]
     (if (= 1 cnt)
       0
       (Math/sqrt (/ (apply + squares) (- cnt 1)))))))


; IMPORT AND PREPARE DATA
(defn convert-value
  "converts a string into the right value of the trip data"
  [trip_key value]
  ((get (into {} trip_types) trip_key) value))

(defn- split-row
  "splits a csv row on the comma"
  [row]
  (clojure.string/split row #","))

(defn- convert-row
  "Takes in a raw imported row of trip data and returns the row with the values
  coerced into the proper data type"
  [row]
  (reduce (fn [row-map [trip-key value]]
            (assoc row-map trip-key (convert-value trip-key value)))
          {}
          (into [] row)))

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
; outside our tolerances, then a {:valid false} map is assoc'd on the row.
; {:verified true} is added to the row after all validations step complete when
; no :valid false is present.

; This is VERY innefficient right now. I'm just learning clojure and focusing on
; getting my head around using the data structures and functions properly

; 1. Numerics
(defn extreme-numeric?
  "determines if value is reasonable to include in analysis. For now extreme
  is defined as 3 times the standard deviation. Magic number 3 is conventional
  with respect to a normal distribution but non verification has been done to
  confirm the values in this data are normal. Be sure to update the README if
  validity criteria change"
  [value mean stddev]
  (> value (+ mean (* 3 stddev))))

(def not-extreme-numeric? (complement extreme-numeric?))

; FIXME: I don't quite have this right. I want to be able to run validity
; functions indempotently and NOT overwrite a :valid false with a :valid true
; and cause problems later when an invalid row is overwritten with false

; needs to contain key && mapkey && have key be false to skip, else process
; TODO: refactor this. I'm incredibly distracted (penny is singing at the top
; of her lungs in the tub) and trying to just get this function correct

(defn add-valid-for-numeric
  "adds {:valid false} for a numeric key iff validity check fails"
  [row mapkey verified-fx?]
  (if (verified-fx? (mapkey row))
    row
    (assoc row :valid false)))

(defn audit-numeric-column!
  [imported-rows column]
  (let [mean   (calc-mean (map column imported-rows))
        stddev (calc-stddev (map column imported-rows) mean)
        passesaudit? #(not-extreme-numeric? % mean stddev)
        ]
    (map (fn [row] (add-valid-for-numeric row column passesaudit?)) imported-rows)))

; TODO: be able to pull out numeric rows from trip_types above
(def numeric-data-columns [:pickup_longitude
                           :pickup_latitude
                           :dropoff_longitude
                           :dropoff_latitude
                           :tip_amount
                           :trip_distance
                           :fare_amount
                           :tip_amount
                           :tolls_amount
                           :total_amount])
(defn audit-numerics
  "Takes in a lazy sequence of rows and verifies values are not extreme"
  ([raw-rows]
   (audit-numerics raw-rows numeric-data-columns))
  ([raw-rows mapkeys]
     (if (first mapkeys)
       (do
         (println (str "running " (first mapkeys)))
         (recur (audit-numeric-column! raw-rows (first mapkeys)) (rest mapkeys)))
       raw-rows)))

; 2. enums
; Columns with known discrete values

(def valid-vendor-values #{1 2})
(def valid-improvement-surcharge  #{0.0 0.3})
(def valid-mta-tax #{0.5})
(def valid-extra #{0.5 1.0})
(def valid-store-and-forward-flags  #{"N" "Y"})
(def valid-rate-code-ids (into #{} (range 1 7))) ; NOTE: 1-6
(def valid-payment-types (into #{} (range 1 7))) ; NOTE: 1-6

(def valid-enum-columns [[valid-vendor-values :vendor_id]
                         [valid-improvement-surcharge :improvement_surcharge]
                         [valid-mta-tax :mta_tax]
                         [valid-extra :extra]
                         [valid-store-and-forward-flags :store_and_fwd_flag]
                         [valid-rate-code-ids :ratecode_id]
                         [valid-payment-types :payment_type]])

(defn audit-enum-column
  "checks and validates that a column has a valid discrete value"
  [rows column valid-set]
  (map
    (fn
      [row]
        (let [col-val (get row column)]
          (if (and col-val (contains? valid-set col-val))
            row
            (assoc row :valid false))))
    rows))

(defn audit-enum
  "Validates rows whose columns should be specified, discrete, values"
  ([rows]
   (audit-enum rows valid-enum-columns))
  ([rows validations]
   (let [[valid-enum-fx mapkey] (first validations)]
     (if (first validations)
       (recur (audit-enum-column rows mapkey valid-enum-fx) (rest validations))
       rows))))

; 3. total row validation
; TODO: some check should happen to validate that the fare, time in cab
; and distance relationship is somewhat rational

(defn dropoff-after-pickup?
  "Asserts that pickup occurred after dropoff"
  [row]
  (.isBefore (:tpep_pickup_datetime row) (:tpep_dropoff_datetime row)))

(defn audit-row-relationship
  [row]
  (if (dropoff-after-pickup? row)
    row
    (assoc row :valid false)))

(defn audit-row-relationship
  "validates that relationships of values in the rows makes sense"
  [rows]
  (map audit-row-relationship rows))

; TODO make a function that takes in a file and runs all the validations

; SCRATCHPAD
; call with (reduce + (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (calc-stddev (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (def first10 (take 10 (import-file filename100)))
; (audit-rows first10)

; (def allrecords (import-file filenameAll))
; (def records100 (import-file filename100))
; (time (map :valid (audit-numerics allrecords)))
