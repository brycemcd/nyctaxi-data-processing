(ns taxidata.main
  "Composed command line utilities"
  (:require
    [taxidata.input-impl-file :as input]
    [taxidata.output-impl-file :as output])
  (:gen-class))

(defn- write-validated-trip-to-file
  [validated-trip valid-outfile invalid-outfile]
  (output/write-trip-to-files validated-trip valid-outfile invalid-outfile))

(defn- validate-trip
  [trip]
  (taxidata.core/validate-trip trip))

(defn -main
  "Takes in file, validates it"
  [infile valid-outfile invalid-outfile]
  (let [tripfx (fn [trip]
                 (write-validated-trip-to-file (validate-trip trip) valid-outfile invalid-outfile))]
    (input/create-trips-from-file infile tripfx)))

