(ns taxidata.core
  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]
            [kixi.stats.core :as stats]
            [taxidata.util-calc :as calc]
            ))

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
  (let [mean   (calc/mean (map column imported-rows))
        stddev (calc/stddev (map column imported-rows) mean)
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

;FIXME: this one is right(def valid-vendor-values #{1 2})
(def valid-vendor-values #{4 2})
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

(def enum-validations
 "provides a vector of [column validation-function] for validation of
 data that should have specific, discrete, values"
 [[:vendor_id valid-vendor-values]
  [:improvement_surcharge valid-improvement-surcharge]
  [:mta_tax valid-mta-tax]
  [:extra valid-extra]
  [:store_and_fwd_flag valid-store-and-forward-flags]
  [:ratecode_id valid-rate-code-ids]
  [:payment_type valid-payment-types]])

(defn validate-enum-cols
  "For a single trip, validate that the values of keys that should be one of a
  specific value is indeed in that set of values. If not, return an invalidated
  trip record"
  [trip]
  (reduce-kv (fn [agg-trip k validation-fx]
               (if (contains? validation-fx (k agg-trip))
                 agg-trip
                 (assoc agg-trip :valid false)
                 ))
             trip
             (into {} enum-validations)))

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
