(ns taxidata.core
  (:require [clj-time.core :as dttm]
            [clj-time.format :as dttm_f]))

;NOTE: it munges the trip_type keys into any order

(def filename "data/trip_2016_06-100.csv")


(def to_int #(Integer. %))
(def to_dec #(Double. %))
;(def to_dttm #(str %)) ; NOTE: mysql formatter is YYYYMMDD HH:mm::ss
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
  [filenm]
  (map #(clojure.string/split % #",")
       (clojure.string/split filenm #"\r\n")))

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

; this is broken around here
(last (mapify (read_trip_file (slurp filename))))
