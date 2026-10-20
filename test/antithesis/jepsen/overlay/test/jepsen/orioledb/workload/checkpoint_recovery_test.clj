(ns jepsen.orioledb.workload.checkpoint-recovery-test
  (:require [clojure.test :refer :all]
            [jepsen.orioledb.workload.checkpoint-recovery :as workload]))

(deftest checksum-is-deterministic-and-null-sensitive
  (is (= "1891ee71937663907782321773960613"
         (workload/md5 [1 2 "payload"])))
  (is (not= (workload/md5 [1 nil "payload"])
            (workload/md5 [1 "" "payload"]))))

(deftest roles-are-stable
  (is (= :checkpoint (workload/role 0)))
  (is (= :recovery (workload/role 1)))
  (is (= :ddl (workload/role 2)))
  (is (= :append (workload/role 3)))
  (is (= :churn (workload/role 4)))
  (is (= :toast (workload/role 5))))

(deftest ddl-state-expectations-are-complete
  (is (= {:scratch? false :column nil :scratch-index? false :churn-index? false}
         (workload/expected-ddl-state 0)))
  (is (= true (:scratch-index? (workload/expected-ddl-state 4))))
  (is (= true (:truncated? (workload/expected-ddl-state 6))))
  (is (= true (:churn-index? (workload/expected-ddl-state 8))))
  (is (thrown? clojure.lang.ExceptionInfo
               (workload/expected-ddl-state 9))))

(deftest generation-classification
  (is (= 12 (workload/boot-number "orioledb-boot-12")))
  (is (nil? (workload/boot-number "not-a-boot"))))

(deftest mismatch-count-normalization
  (is (workload/zero-values? {:append_diff 0 :churn_diff 0 :toast_diff 0}))
  (is (not (workload/zero-values? {:append_diff 0 :churn_diff 1 :toast_diff 0}))))

(deftest retryable-errors
  (is (workload/retryable? (java.sql.SQLException. "closed" "08006")))
  (is (workload/retryable? (java.net.SocketException. "connection reset")))
  (is (not (workload/retryable? (java.sql.SQLException. "bad data" "23514")))))
