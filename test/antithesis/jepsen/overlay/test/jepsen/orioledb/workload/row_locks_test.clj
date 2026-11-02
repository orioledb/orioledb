(ns jepsen.orioledb.workload.row-locks-test
  (:require [clojure.test :refer :all]
            [jepsen.orioledb.workload.row-locks :as workload]))

(deftest compatibility-matrix-is-complete-and-symmetric
  (is (= 16 (count workload/compatibility)))
  (doseq [left workload/lock-modes
          right workload/lock-modes]
    (is (boolean? (get workload/compatibility [left right])))
    (is (= (get workload/compatibility [left right])
           (get workload/compatibility [right left])))))

(deftest compatibility-matrix-matches-postgresql
  (is (workload/compatibility [:key-share :no-key-update]))
  (is (workload/compatibility [:share :share]))
  (is (not (workload/compatibility [:key-share :update])))
  (is (not (workload/compatibility [:share :no-key-update])))
  (is (not (workload/compatibility [:update :update]))))

(deftest sql-state-classification-is-narrow
  (is (workload/lock-unavailable?
       (java.sql.SQLException. "could not obtain lock" "55P03")))
  (is (not (workload/lock-unavailable?
            (java.sql.SQLException. "deadlock" "40P01"))))
  (is (workload/retryable?
       (java.sql.SQLException. "connection closed" "08006")))
  (is (not (workload/retryable?
            (java.sql.SQLException. "serialization" "40001")))))

(deftest roles-cover-every-actor
  (is (= [:compatibility :savepoint :counter :item :foreign-key :lock-only]
         (mapv workload/role (range 6)))))

(deftest table-am-is-validated
  (is (= 8 (count (workload/schema-statements "orioledb"))))
  (is (= 8 (count (workload/schema-statements "heap")))))
