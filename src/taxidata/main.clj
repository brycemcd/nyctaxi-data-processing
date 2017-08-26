(ns taxidata.main
  "Composed command line utilities"
  (:require
    [taxidata.input-impl-file :as input]
    [taxidata.output-impl-file :as output])
  (:gen-class))

(defn- write-validated-trip-to-file
  "Writes a validated trip to one of two files. Depends on the :valid key
  being set in the validation step"
  [validated-trip valid-outfile invalid-outfile]
  (output/write-trip-to-files validated-trip valid-outfile invalid-outfile))

(defn- validate-trip
  "validates a freshly created trip record"
  [trip]
  (taxidata.core/validate-trip trip))

(defn -main
  "Main invocation for this code as a command line application. Usage:
  `java -jar target/taxidata-<version>-standalone.jar path/to/infile path/to/valid/trips.csv path/to/invalid-trips.csv`

  Infile is a csv file of taxi trips downloaded from the TLC site

  Valid and Invalid trips are stored as a csv"
  [infile valid-outfile invalid-outfile]
  (let [tripfx (fn [trip]
                 (write-validated-trip-to-file (validate-trip trip) valid-outfile invalid-outfile))]
    (input/create-trips-from-file infile tripfx)))

