(ns taxidata.core-test
  (:require [clojure.test :refer :all]
            [taxidata.core :refer :all]))

(deftest to_int-test
  (testing "takes in a value and coerces it to an int when possible or throws
           an error if not possible"
    (is (= 0 (to_int "0")))
    (is (thrown? java.lang.NumberFormatException (to_int "snorkle")))))

(deftest to_dec-test
  (testing "takes in a value and coerces it to a fixed width decimal or throws
           an error if not possible"
    (is (= 0.0 (to_dec "0.0")))
    (is (thrown? java.lang.NumberFormatException (to_dec "snorkle")))))

(deftest convert-value-test
  (testing "given a trip key and a value, converts the value into the right thing
           from a string"
    (is (= 1 (convert-value :vendor_id "1")))
    (is (= 0.0 (convert-value :extra "0.0")))))

(with-test
  (def rows (import-file "data/trip_2016_06-10.csv"))
  (def first-row (first rows))

  (testing "after file is imported, data in file is converted as a map"
    (is (instance? clojure.lang.PersistentHashMap first-row))

    ; NOTE: these are extremely brittle tests, but I'm not sure of another way
    ; of writing these without making them really difficult to read/reason about
    (is (= 2 (:vendor_id first-row)))
    (is (= (to_dttm "2016-06-09 21:06:36") (:tpep_pickup_datetime first-row)))
    (is (= (to_dttm "2016-06-09 21:13:08") (:tpep_dropoff_datetime first-row)))
    (is (= 2 (:passenger_count first-row)))
    (is (= 0.79 (:trip_distance first-row)))
    (is (= -73.983360290527344 (:pickup_longitude first-row)))
    (is (= 40.760936737060547 (:pickup_latitude first-row)))
    (is (= -73.977462768554688 (:dropoff_longitude first-row)))
    (is (= 40.753978729248047 (:dropoff_latitude first-row)))
    (is (= 1 (:ratecode_id first-row)))
    (is (= "N" (:store_and_fwd_flag first-row)))
    (is (= 2 (:payment_type first-row)))
    (is (= 6.0 (:fare_amount first-row)))
    (is (= 0.5 (:extra first-row)))
    (is (= 0.5 (:mta_tax first-row)))
    (is (= 0.0 (:tip_amount first-row)))
    (is (= 0.0 (:tolls_amount first-row)))
    (is (= 0.3 (:improvement_surcharge first-row)))
    (is (= 7.3 (:total_amount first-row)))

    (is (= 9 (count rows)))))

(deftest calc-mean-test
  (testing "when count is 0"
    (is (= 0.0 (calc-mean {}))))

  (testing "when count is more than 0"
    (is (= 2 (calc-mean [1 2 3])))
    (is (= 3.02469 (calc-mean [1 2 3 4 5.12345]))))) ; making sure sigfigs render

(deftest calc-stddev-test
  (testing "calculates the standard deviation of a sample and the mean when not
            passed a mean (optimization, assumes mean is correct)"
    (is (= 1.8708286933869707 (calc-stddev [1 2 3 4 5 6] 7/2)))
    (is (= 1.8708286933869707 (calc-stddev [1 2 3 4 5 6]))))
  (testing "does not divide by zero"
    (is (= 0 (calc-stddev [1])))))
