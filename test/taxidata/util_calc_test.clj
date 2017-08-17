(ns taxidata.util-calc-test
  (:require [clojure.test :refer :all]
            [taxidata.util-calc :refer :all]
            [bond.james :as bond :refer [with-spy]]))

(deftest mean-test
  (testing "when count is 0"
    (is (= 0.0 (mean {}))))

  (testing "when count is more than 0"
    (is (= 2 (mean [1 2 3])))
    (is (= 3.02469 (mean [1 2 3 4 5.12345]))))) ; making sure sigfigs render

(deftest stddev-test
  (testing "calculates the standard deviation of a sample and the mean when not
            passed a mean (optimization, assumes mean is correct)"
    (is (= 1.8708286933869707 (stddev [1 2 3 4 5 6] 7/2)))
    (is (= 1.8708286933869707 (stddev [1 2 3 4 5 6]))))
  (testing "does not divide by zero"
    (is (= 0 (stddev [1])))))

(deftest extreme-numeric?-test
  (testing "when a value is > 3 standard deviations from the mean, it's extreme"
    (is (= false (extreme-numeric? 3 3 1)))
    (is (= true (extreme-numeric? 6.1 3 1))))
  (testing "complement function exists"
    (is (= true (not-extreme-numeric? 3 3 1)))))
