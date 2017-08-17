(ns taxidata.util-calc
  "Provides calculation functions on collections of data")

; BASIC MATHS
; The calc-functions here are designed to provide easily accessible calculations
; useful for verifying and auditing rows later

; TODO: take into account missing fields

; https://github.com/clojure-cookbook/clojure-cookbook/blob/master/01_primitive-data/1-20_simple-statistics.asciidoc
(defn mean
  "calculate a simple mean of a collection"
  [coll]
  (let [sum (apply + coll)
        cnt (count coll)]
    (if (pos? cnt)
      (/ sum cnt)
      0.0)))

(defn- x-avg-squared
  [x avg]
  (* (- x avg) (- x avg)))

(defn stddev
  "calculate the stddev of a collection"
  ([coll]
    (stddev coll (mean coll)))
  ([coll avg]
   (let [squares (map #(x-avg-squared % avg) coll)
         cnt (count coll)]
     (if (= 1 cnt)
       0
       (Math/sqrt (/ (apply + squares) (- cnt 1)))))))

(defn extreme-numeric?
  "determines if value is reasonable to include in analysis. For now extreme
  is defined as 3 times the standard deviation. Magic number 3 is conventional
  with respect to a normal distribution but non verification has been done to
  confirm the values in this data are normal. Be sure to update the README if
  validity criteria change"
  [value mean stddev]
  (> value (+ mean (* 3 stddev))))

(def not-extreme-numeric? (complement extreme-numeric?))
