(ns taxidata.core
  "NOTE: the approach for verifying is to take the raw row and pass over a series
  of filters that will detect invalid values. If a row is found to have values
  outside our tolerances, then a {:valid false} map is assoc'd on the row.
  {:valid true} is added to the row after all validations are complete and when
  no :valid false is present.

  This is VERY innefficient right now. I'm just learning clojure and focusing on
  getting my head around using the data structures and functions properly"

  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]
            [kixi.stats.core :as stats]
            [taxidata.util-calc :as calc]
            ))

; 0. Invalidation
(defn invalidate
  "Given a map, append :valid false and the reason for the invalidation to
  the map."
  [trip invalid-reason]
  (let [reasons (or (:invalid-reason trip) '())]
    (assoc trip :valid false :invalid-reason (cons invalid-reason reasons))))

; 1. Numerics

; TODO: This has to be re-done to use pre-calculated means and stddevs and then
; have a function that is able to audit a single row

; TODO: be able to pull out numeric rows from input-impl-file/trip-types
; TODO: this hash should be calulated or a configuration value
; NOTE: these are completely contrived values right now
(def numeric-validations
  "A hash returning relevant values for validating the continuous numeric
  values"
  {:pickup_longitude {:mean 40.760936737060547 :stddev 2}
   :pickup_latitude {:mean -73.983360290527344 :stddev 2}
   :dropoff_longitude {:mean 40.760936737060547 :stddev 2}
   :dropoff_latitude {:mean -73.983360290527344 :stddev 2}
   :tip_amount {:mean 10 :stddev 3}
   :trip_distance {:mean 2 :stddev 2}
   :fare_amount {:mean 2 :stddev 2}
   :tolls_amount {:mean 2 :stddev 2}
   :total_amount {:mean 2 :stddev 2}
   })

(defn audit-continuous-key
  "Checks the value of a key is within the tolerances we've defined and
  invalidates extreme values. Designed to prevent analyzing values from
  technical defects in the taxi meter or accidental hand-key inputs"
  [trip column]
  (if (calc/extreme-numeric? (column trip)
                             (get-in numeric-validations [column :mean])
                             (get-in numeric-validations [column :stddev]))
    (invalidate trip column)
    trip))

(defn validate-trip-continuous
  "For a single trip, validate that the values of keys that are continuous are
  indeed not extreme. If not, return an invalidated trip record"
  [trip]
  (reduce (fn [agg-trip k]
            (audit-continuous-key agg-trip k))
          trip
          (keys numeric-validations)))

; 2. enums
; Columns with known discrete values

(def valid-vendor-values #{1 2})
(def valid-improvement-surcharge  #{0.0 0.3})
(def valid-mta-tax #{0.5})
(def valid-extra
  "The data dictionary indicates 0.5 and 1.0 are valid values and kind of
  suggests that 0.0 might be valid as well. ~ 1/2 the data include 0.0 so I'll
  assume it's valid for now"
  #{0.0 0.5 1.0})

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

(defn validate-trip-enum
  "For a single trip, validate that the values of keys that should be one of a
  specific value is indeed in that set of values. If not, return an invalidated
  trip record"
  ([trip]
   (validate-trip-enum trip enum-validations))

  ([trip validations]
   (reduce-kv (fn [agg-trip k validation-set]
                (audit-enum-key agg-trip k validation-set))
              trip
              validations)))


; 3. total trip validation
; TODO: some check should happen to validate that the fare, time in cab
; and distance relationship is somewhat rational

(defn dropoff-after-pickup?
  "Asserts that pickup occurred after dropoff"
  [trip]
  (.isBefore (:tpep_pickup_datetime trip) (:tpep_dropoff_datetime trip)))

(defn validate-trip-relationship
  [trip]
  (if (dropoff-after-pickup? trip)
    trip
    (invalidate trip :whole-trip-validation)))

; 4. Compose all auditing functions together
(defn validate-trip
  "Higher order function to run all validations on a trip record. If all
  validations pass, then {:valid true} is assigned"
  [trip]
  (let [validated-trip (-> trip
                           validate-trip-enum
                           validate-trip-continuous
                           validate-trip-relationship)]
    (if (contains? validated-trip :valid)
      validated-trip
      (assoc validated-trip :valid true))))
