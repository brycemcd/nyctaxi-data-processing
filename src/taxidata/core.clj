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

; TODO: This has to be re-done to use pre-calculated means and stddevs and then
; have a function that is able to audit a single row

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

(def enum-validations
 "provides a vector of [column validation-function] for validation of
 data that should have specific, discrete, values"
 {:vendor_id valid-vendor-values
  :improvement_surcharge valid-improvement-surcharge
  :mta_tax valid-mta-tax
  :extra valid-extra
  :store_and_fwd_flag valid-store-and-forward-flags
  :ratecode_id valid-rate-code-ids
  :payment_type valid-payment-types})

(defn audit-enum-key
  "checks and validates that a key has a valid discrete value"
  [trip column valid-set]
  (let [col-val (get trip column)]
    (if (and col-val (contains? valid-set col-val))
      trip
      (invalidate trip column))))

(defn validate-trip-enum-keys
  "For a single trip, validate that the values of keys that should be one of a
  specific value is indeed in that set of values. If not, return an invalidated
  trip record"
  [trip validations]
  (reduce-kv (fn [agg-trip k validation-set]
               (audit-enum-key agg-trip k validation-set))
             trip
             validations))


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
  (let [audited-rows ((comp audit-rows-relationship) rows)]
  (map (fn [row]
         (if (contains? row :valid)
           row
           (assoc row :valid true)))
         audited-rows)))
