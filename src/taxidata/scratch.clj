(ns taxidata.scratch
  "An area where I can dump silly scripts to check other functions I'm writing
  in core"
  (:require
            ;[taxidata.core :as core]
            [taxidata.input-impl-file :as taxiio]
            ))
;NOTE: it munges the trip_type keys into any order

(def filename9  "data/trip_2016_06-10.csv")
(def filename99 "data/trip_2016_06-100.csv")
(def filename1000000 "data/trip_2016_06-1000000.csv")
(def filenameAll "data/yellow_tripdata_2016-06.csv")

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






; 2017-08-20
; read in the file, print valid records to a file and invalid to another file
;(def valid-file (clojure.java.io/writer "valid.csv"))
;(def invalid-file (clojure.java.io/writer "invalid.csv"))
;(.close valid-file)
;(.close invalid-file)

;(def all-returns (atom ()))
;(defn callbk
  ;[trip]
  ;(swap! all-returns conj trip))

;(def validrecords99 (reverse (reduce conj
                                     ;()
                                     ;(taxidata.input-impl-file/create-trips-from-file filename99 callbk))))

;(def first-row (last @all-returns))
(def valid-file "valid.csv")
(def invalid-file "invalid.csv")

(defn stringify-column
  [trip col]
  (if (= col :invalid-reason)
    (reduce (fn [aggr col] (str aggr col)) "" (col trip))
    (col trip)))

(defn trip-to-csv
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
(defn read-validate-csv
  [trip]
  (let [valtrip (taxidata.core/validate-trip trip)]
     (println (str (trip-to-csv valtrip) "\n"))))

(defn write-trip-to-file
  [trip]
  (let [val-trip (taxidata.core/validate-trip trip)
        file (if (:valid val-trip) valid-file invalid-file)]
     (spit file (str (trip-to-csv val-trip) "\n") :append true)))

; invocation:
; be sure to truncate the valid and invalid files
; (require 'taxidata.scratch :reload)
;(taxiio/create-trips-from-file filename9 write-trip-to-file)
;(taxiio/create-trips-from-file filename9 read-validate-csv)
