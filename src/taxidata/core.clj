(ns taxidata.core
  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]
            [kixi.stats.core :as stats]
            ))

(declare mapify)

;NOTE: it munges the trip_type keys into any order

(def filename100 "data/trip_2016_06-100.csv")
(def filename1000000 "data/trip_2016_06-1000000.csv")
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
(def trip-types [[:vendor_id to_int]
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

(def trip-header (map first trip-types))



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
  ((get (into {} trip-types) trip_key) value))

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
                (cons (convert-row (zipmap trip-header (split-row line))) (helper rdr))
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

; 0. Invalidation
(defn invalidate
  "Given a map, append :valid false and the reason for the invalidation to
  the map."
  [row invalid-reason]
  (let [reasons (or (:invalid-reason row) '())]
    (assoc row :valid false :invalid-reason (cons invalid-reason reasons))))

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
    (invalidate row mapkey)))

(defn audit-numeric-column!
  [imported-rows column]
  (let [mean   (calc-mean (map column imported-rows))
        stddev (calc-stddev (map column imported-rows) mean)
        passesaudit? #(not-extreme-numeric? % mean stddev)
        ]
    (map (fn [row] (add-valid-for-numeric row column passesaudit?)) imported-rows)))

; TODO: be able to pull out numeric rows from trip-types above
(def numeric-data-columns [:pickup_longitude
                           :pickup_latitude
                           :dropoff_longitude
                           :dropoff_latitude
                           :tip_amount
                           :trip_distance
                           :fare_amount
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
; NOTE: the data dictionary indicates 0.5 and 1.0 are valid values and kind of
; suggests that 0.0 might be valid as well. ~ 1/2 the data include 0.0 so I'll
; assume it's valid for now
(def valid-extra #{0.0 0.5 1.0})
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
            (invalidate row column))))
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
    (invalidate row :whole-row-validation)))

(defn audit-rows-relationship
  "validates that relationships of values in the rows makes sense"
  [rows]
  (map audit-row-relationship rows))

; 4. Compose all auditing functions together
(defn validate-rows
  "pass in a seq of rows and return all rows after having passed through all
  auditing functions. Returned rows will have valid: true if validated and
  :valid false if any audit function failed"
  [rows]
  (let [audited-rows ((comp audit-rows-relationship audit-enum audit-numerics) rows)]
  (map (fn [row]
         (if (contains? row :valid)
           row
           (assoc row :valid true)))
         audited-rows)))

; SCRATCHPAD

; call with (reduce + (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (calc-stddev (map to_int (map first (mapify-row (lazy-file-lines filename)))))
; (def first10 (take 10 (import-file filename100)))
; (audit-rows first10)

; (def allrecords (import-file filenameAll))
; (def records100 (import-file filename100))
; (def records1M (import-file filename1000000))
; (map println (filter #(= false (:valid %)) (validate-rows validrecords99)))
; (frequencies (flatten (map :invalid-reason (filter #(= false (:valid %)) (validate-rows records1M)))))
; NOTE: 2017-07-05
; Running the above frequencies function yeilds this:
; {
; :tip_amount 42658,
; :tolls_amount 3212
; :extra 449637
; :improvement_surcharge 459
; :pickup_longitude 12566
; :ratecode_id 16
; :trip_distance 29314
; :dropoff_longitude 11659
; :fare_amount 13
; :mta_tax 4987
; :total_amount 20
; :whole-row-validation 1109}
; the "extra" column is an enum with recorded valid values of 0.5 and 1.0
; (map println (sort-by last (frequencies (map :extra records1M))))
; [2.0 1]
; [0.3 1]
; [70.0 1]
; [2.5 1]
; [10.0 1]
; [20.0 1]
; [1.5 1]
; [50.0 1]
; [30.3 1]
; [0.8 1]
; [34.59 1]
; [0.03 1]
; [1.23 1]
; [4.54 1]
; [0.02 2]
; [5.5 3]
; [0.1 4]
; [-4.5 7]
; [-1.0 55]
; [-0.5 181]
; [4.5 4252]
; [1.0 157483]
; [0.5 392880]
; [0.0 445119]
; I think 0.0 is a valid value and I have no idea what the other values could be
; from the numeric validation
; (frequencies (flatten (map :invalid-reason (filter #(= false (:valid %)) (validate-rows records1M)))))
;
; Much better. The next highest invalid column is tip_amount
; tip_amount is a continuous column. The mean of the invalid rows is 12.985 with
; stddev of 7.27 That doesn't seem that high to me. It's reasonable that someone
; was coming to/from the airport. $90 fare * 20% tip would be an $18 tip
; the mean tip amount of valid values is 1.6578 with a stddev of 1.8384
; just out of sheer curiosity, the mean trip distance for valid tip_amounts is
; 2.7700 and the mean trip distance of invalid tip_amounts is 15.103. I think
; this is a false positive. There are probably enough long trips to justify the
; large tip amount. I'll move the valid numeric out to 4 stddev to see what that
; does
;
; Still makes sense. Mean fare_amount = 58.3366 with a mean tip amount of 17.36
; for the invalid tips. That's a 30% tip and well within reason
;
; Even at 5 stddev as the definistion of extreme numeric, the mean trip_distance
; is 16.0377, 22.344 mean tip and 67.744 mean fare_amount. This is pretty reasonable
; I think it would be fairly inconsequential to exclude these trips for now
; and come up with a fare / distance / tip algo to detect extreme values. It's
; likely a multi-modal distribution and auditing the data should appreciate that
; phenomenon
