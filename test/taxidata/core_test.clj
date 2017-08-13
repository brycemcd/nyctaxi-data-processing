(ns taxidata.core-test
  (:require [clojure.test :refer :all]
            [taxidata.core :refer :all]
            [taxidata.scratch :refer :all]
            [taxidata.input-impl-file :refer :all]
            [bond.james :as bond :refer [with-spy]]))

;; NOTE: 99 records is helpful here to avoid influencing the mean and stddev
(def validrecords99 (import-file filename99))

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

(deftest invalidate-test
  (with-test
    (def example-row {:foo "bar"})
    (def invalid-col :key1)
    (def invalid-col2 :key2)

    (testing "assoc :valid false to the map"
      (is (= false (:valid (invalidate example-row invalid-col)))))

    (testing "when no invalid reasons exist, A new key and list are created"
      (let [return-map (invalidate example-row invalid-col)
            invalid-reasons (:invalid-reason return-map)]
        (is (= invalid-col (first invalid-reasons)))
        (is (= 1 (count invalid-reasons)))))

    (testing "when invalid reasons already exist, A new key and vector are created"
      (let [return-map (invalidate (assoc example-row :valid false :invalid-reason (cons invalid-col2 '())) invalid-col)
            invalid-reasons (:invalid-reason return-map)]
        (is (= invalid-col (first invalid-reasons)))
        (is (= invalid-col2 (second invalid-reasons)))
        (is (= 2 (count invalid-reasons)))))))

(deftest extreme-numeric?-test
  (testing "when a value is > 3 standard deviations from the mean, it's extreme"
    (is (= false (extreme-numeric? 3 3 1)))
    (is (= true (extreme-numeric? 6.1 3 1))))
  (testing "complement function exists"
    (is (= true (not-extreme-numeric? 3 3 1)))))

; add-valid-for-numeric-test
(with-test
  (def valid-value "bar")
  (def invalid-value "baz")
  (def valid-fx #(= valid-value %))

  (testing "adds {:valid false} when the validity check fails"
    (is (contains? (add-valid-for-numeric {:foo invalid-value} :foo valid-fx) :valid))
    (is (not (contains? (add-valid-for-numeric {:foo valid-value} :foo valid-fx) :valid)))))

(with-test
  (def invalidrecords99 (cons (assoc (first validrecords99) :tip_amount 70) validrecords99 ))

  (testing "audit-numberic-column!"
    (testing "tests a column for extreme continuous values"
      (is (contains? (first (audit-numeric-column! invalidrecords99 :tip_amount)) :valid)))))

(deftest audit-numerics-test
  (testing "a higher order, more specific, fx to check all known continuous
           numeric functions for extreme values"
    (with-spy [audit-numeric-column!]
      (audit-numerics validrecords99)
      (let [calls (bond/calls audit-numeric-column!)]
        (is (= numeric-data-columns (map last (map :args calls))))
        (is (= (count numeric-data-columns) (count calls)))))))

(with-test
  (def validation-column :vendor_id)
  (def invalidrecords99 (cons (assoc (first validrecords99) validation-column 3) validrecords99 ))
  (def valid-set #{1 2})

  (testing "audit-enum-column"
    (testing "adds {:valid false} when the map key is not present"
      (let [invalid-rows (filter
                           (fn [row] (contains? row :valid))
                           (audit-enum-column validrecords99 :foo valid-set))]
      (is (= (count validrecords99) (count invalid-rows)))))

    (testing "adds {:valid false} when the value of the column is not in the verification set"
      (let [invalid-rows (filter
                           (fn [row] (contains? row :valid))
                           (audit-enum-column invalidrecords99 validation-column valid-set))]
      (is (= 1 (count invalid-rows)))))

    (testing "does not add any :valid keys to the map if the rows are valid"
      (let [invalid-rows (filter
                           (fn [row] (contains? row :valid))
                           (audit-enum-column validrecords99 validation-column valid-set))]
      (is (= 0 (count invalid-rows)))))))

(deftest audit-enum-test
  (testing "a higher order, more specific, fx to check all known discrete columns"

    (testing "unary version calls is legal and calls binary version of itself"
      (with-spy [audit-enum-column]
        (audit-enum validrecords99)

        (let [calls (bond/calls audit-enum-column)]
          (testing "all enum columns are passed in to audit-enum"
            (is (= (map last valid-enum-columns) (map second (map :args calls)))))
          (testing "valid-enum-columns is called for each column specified"
            (is (= (count valid-enum-columns) (count calls)))))))

    (testing "binary version can be called directly"
      (with-spy [audit-enum-column]
        (audit-enum validrecords99 [[valid-vendor-values :vendor_id]])

        (let [calls (bond/calls audit-enum-column)]
          (testing "all enum columns are passed in to audit-enum"
            (is (= [:vendor_id] (map second (map :args calls)))))
          (testing "valid-enum-columns is called for each column specified"
            (is (= 1 (count calls)))))))))

(deftest dropoff-after-pickup?-test
  (with-test
    (def validdropoff {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                       :tpep_dropoff_datetime (to_dttm "2017-07-02 19:00:00")})

    (testing "dropoff-after-pickup? validation to ensure dropoff time is later than pickup time"
      (is (= true (dropoff-after-pickup? validdropoff)))))

  (with-test
    (def invaliddropoff {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                         :tpep_dropoff_datetime (to_dttm "2017-07-02 17:00:00")})

    (testing "dropoff-after-pickup? validation to ensure dropoff time is later than pickup time"
      (is (= false (dropoff-after-pickup? invaliddropoff))))))

; FIXME: for reasons I can't comprehend, audit-row-relationship-test NEVER returns failed tests (false positive!)
(deftest audit-row-test
  (with-test
    (def dropoff-before-pickup {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                                :tpep_dropoff_datetime (to_dttm "2017-07-02 14:00:00")})

    (def dropoff-after-pickup  {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                                :tpep_dropoff_datetime (to_dttm "2017-07-02 20:00:00")})
    (testing "row is invalidated if validation functions return false"
      (is (= true (contains? (audit-row-relationship dropoff-before-pickup) :valid))))

    (testing "row is validated if validation functions return true"
      (is (= false (contains? (audit-row-relationship dropoff-after-pickup) :valid))))))

(deftest audit-rows-relationship-test
  (testing "when called with a seq of rows, each row is audited"
    (with-spy [audit-row-relationship]
      ; count is called here to realize the map function
      (count (audit-rows-relationship validrecords99))

      (testing "all rows are passed to audit functions"
        (let [calls (bond/calls audit-row-relationship)]
          (is (= (count validrecords99) (count calls))))))))

(deftest validatate-rows-test
    (testing "after calling all audit functions, :valid is assoc'd to the row"
      ; NOTE: this just checks the count of not nil :valid keys. Not a great
      ; test. Still learning Clojure
      (is (= 99 (count (filter #(not (= nil %)) (map :valid (validate-rows validrecords99))))))))
