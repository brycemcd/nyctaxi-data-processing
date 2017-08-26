(ns taxidata.output-impl-file
  "Outputs validated trips to files"
  (:require
            [clj-time.format :as dttm_f]
  ))


(defn- stringify-column
  [trip col]
  (if (= col :invalid-reason)
    (reduce (fn [aggr col] (str aggr col)) "" (col trip))
    (col trip)))

(defn- trip-to-csv
  [trip]
  (let [columns [:vendor_id
                 :tpep_pickup_datetime
                 :tpep_dropoff_datetime
                 :passenger_count
                 :trip_distance
                 :pickup_longitude
                 :pickup_latitude
                 :ratecode_id
                 :store_and_fwd_flag
                 :dropoff_longitude
                 :dropoff_latitude
                 :payment_type
                 :fare_amount
                 :extra
                 :mta_tax
                 :tip_amount
                 :tolls_amount
                 :improvement_surcharge
                 :total_amount
                 :valid
                 :invalid-reason]]
    (reduce (fn [aggr col]
              (if (= aggr "")
                (stringify-column trip col)
                (str aggr "," (stringify-column trip col))))
            ""
            columns)))

(defn write-trip-to-files
  ;{^:notest}
  "Write a validated trip to a file. If it's a valid trip, write it to
  `valid-file`. If it's an invalid trip, write it to `invalid-file`."
  [trip valid-file invalid-file]
  (let [file (if (:valid trip) valid-file invalid-file)]
     (spit file (str (trip-to-csv trip) "\n") :append true)))
