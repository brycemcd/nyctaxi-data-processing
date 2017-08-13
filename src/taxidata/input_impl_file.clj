(ns taxidata.input-impl-file
  "Imports taxi trips from a file. Files are expected to be UTF-8 encoded and
  records are expected to be separated by new lines (\n) and values are
  expected to be comma separated files"
  (:require
            [taxidata.core :refer :all]
  ))

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
  "Imports a file and converts each row into a map with properly typed values.
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
