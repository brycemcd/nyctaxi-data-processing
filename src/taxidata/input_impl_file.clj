(ns taxidata.input-impl-file
  "Imports taxi trips from a file. Files are expected to be UTF-8 encoded and
  records are expected to be separated by new lines (\r\n) and values are
  expected to be comma separated files"
  (:require
            [clj-time.format :as dttm_f]
            [taxidata.core :refer :all]
  ))

; NOTE: if/when another input type is created, these functions and trip-types
; should be moved to some sort of utility namespace. I found it to be more
; convenient to just leave them here for now and avoid having lots of windows
; open at the same time searching for these
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
(def trip-types
  "column names and primitives expected of the data in an ordered data structure"
  [[:vendor_id to_int]
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

(def trip-header
 "Just the keys of trip-types. Should be the same as a header in a data file"
 (map first trip-types))

(defrecord TaxiTrip
  [vendor_id tpep_pickup_datetime tpep_dropoff_datetime
   passenger_count trip_distance pickup_longitude pickup_latitude
   ratecode_id store_and_fwd_flag dropoff_longitude dropoff_latitude
   payment_type fare_amount extra mta_tax tip_amount tolls_amount
   improvement_surcharge total_amount])

(defn- split-row
  "splits a csv row on the comma"
  [row]
  (clojure.string/split row #","))

(defn- cast-keypair
  "converts a key's value into the proper type and returns the key and value
  i.e. {:a '123'} -> {:a 123}"
  [[k v]]
  (let [mapd-types (into {} trip-types)]
    {k ((k mapd-types) v)}))

(defn- cast-row
  [mapd-row]
  (into {} (map cast-keypair mapd-row)))

(defn is-file-header
  "if raw line matches the known header value of VendorID, then this line
  is header line"
  [line]
  (re-matches #"VendorID.*" line))

(defn create-trip
  "creates a TaxiTrip from a raw line of data in the file. If line passed in
  is a header line, it should be discarded"
  [line]
  ; convert the raw row into a map, then TaxiTrip with the wrong types cast
  (if-not (is-file-header line)
    (->> line
         split-row
         (zipmap trip-header)
         cast-row
         map->TaxiTrip)))

; NOTE: feels like I want a transducer here
(defn create-trips-from-file
  "Buffers a file, creates TaxiTrips from each row in the file (default line
  separator as used in clojure.java.io/reader), calls a function on the row and
  then a callback on the result of the rowfx

  callbackfx = a function to call after the tripfx has been called on the TaxiTrip"
  [file callbackfx]
  (with-open [rdr (clojure.java.io/reader file)]
    (doseq [line (line-seq rdr)]
      (if-let [not-header (create-trip line)]
        (callbackfx not-header)))))
