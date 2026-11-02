(ns jepsen.orioledb.workload.row-locks
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.postgres.client :as pg]
            [jepsen.random :as random]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.nio.file Files StandardCopyOption)
           (java.sql Connection SQLException)
           (java.time Instant)))

(def schema-version 1)
(def state-dir (or (System/getenv "RUNTIME_STATE_DIR") "/runtime-state"))
(def builder {:builder-fn rs/as-unqualified-lower-maps})

(def lock-modes
  [:key-share :share :no-key-update :update])

(def lock-mode-sql
  {:key-share "KEY SHARE"
   :share "SHARE"
   :no-key-update "NO KEY UPDATE"
   :update "UPDATE"})

;; true means that holder and contender can coexist on one tuple.
(def compatibility
  {[:key-share :key-share] true
   [:key-share :share] true
   [:key-share :no-key-update] true
   [:key-share :update] false
   [:share :key-share] true
   [:share :share] true
   [:share :no-key-update] false
   [:share :update] false
   [:no-key-update :key-share] true
   [:no-key-update :share] false
   [:no-key-update :no-key-update] false
   [:no-key-update :update] false
   [:update :key-share] false
   [:update :share] false
   [:update :no-key-update] false
   [:update :update] false})

(def initial-metrics
  {:counter-writes 0
   :lock-only-transactions 0
   :whole-rollbacks 0
   :savepoint-rollbacks 0
   :item-writes 0
   :fk-writes 0
   :compatibility-probes 0
   :savepoint-probes 0
   :expected-lock-conflicts 0
   :operation-failures 0})

(defonce metrics (atom initial-metrics))
(defonce covered-pairs (atom #{}))
(defonce covered-modes (atom #{}))
(defonce covered-savepoints (atom #{}))
(defonce operation-count (atom 0))
(defonce compatibility-index (atom -1))
(defonce savepoint-index (atom -1))

(defn bump! [key]
  (swap! metrics update key (fnil inc 0)))

(defn sql-state [^Throwable error]
  (when (instance? SQLException error)
    (.getSQLState ^SQLException error)))

(defn retryable? [^Throwable error]
  (or (and (instance? SQLException error)
           (let [state (sql-state error)]
             (or (and state (str/starts-with? state "08"))
                 (#{"57P01" "57P02" "57P03"} state))))
      (some? (re-find #"(?i)connection.*(closed|refused|reset)|broken pipe|socket|I/O error"
                      (or (.getMessage error) "")))))

(defn lock-unavailable? [^Throwable error]
  (= "55P03" (sql-state error)))

(defn atomic-json! [name value]
  (let [target (.toPath (io/file state-dir name))
        temporary (.toPath (io/file state-dir (str name ".tmp." (System/nanoTime))))]
    (.mkdirs (io/file state-dir))
    (spit (.toFile temporary) (json/generate-string value))
    (Files/move temporary target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/REPLACE_EXISTING
                             StandardCopyOption/ATOMIC_MOVE]))))

(defn environment-test []
  {:node (or (System/getenv "JEPSEN_NODE") "orioledb")
   :postgres-user (or (System/getenv "JEPSEN_POSTGRES_USER") "jepsen")
   :postgres-password (or (System/getenv "JEPSEN_POSTGRES_PASSWORD") "pw")
   :postgres-port (parse-long (or (System/getenv "JEPSEN_POSTGRES_PORT") "5432"))
   :postgres-sslmode "disable"})

(defn open-retrying
  ([test] (open-retrying test 180))
  ([test attempts]
   (loop [remaining attempts]
     (let [result (try
                    {:connection (pg/open test (:node test))}
                    (catch Throwable error {:error error}))]
       (if-let [connection (:connection result)]
         connection
         (let [error (:error result)]
           (if (and (retryable? error) (pos? remaining))
             (do (Thread/sleep 1000) (recur (dec remaining)))
             (throw error))))))))

(defn close-quietly! [connection]
  (when connection
    (try (.close ^Connection connection) (catch Throwable _))))

(defn execute-one! [connection statement]
  (jdbc/execute-one! connection statement builder))

(defn rollback-quietly! [connection]
  (when connection
    (try (.rollback ^Connection connection) (catch Throwable _))))

(defn with-transaction! [^Connection connection f]
  (let [auto-commit (.getAutoCommit connection)]
    (try
      (.setAutoCommit connection false)
      (let [result (f connection)]
        (.commit connection)
        result)
      (catch Throwable error
        (rollback-quietly! connection)
        (throw error))
      (finally
        (try (.setAutoCommit connection auto-commit) (catch Throwable _))))))

(defn lock-row! [connection table key mode & [nowait?]]
  (swap! covered-modes conj mode)
  (jdbc/execute! connection
                 [(str "SELECT id FROM " table " WHERE id = ? FOR "
                       (lock-mode-sql mode) (when nowait? " NOWAIT")) key]
                 builder))

(def compatibility-pairs
  (vec (for [holder lock-modes contender lock-modes]
         [holder contender])))

(defn next-compatibility-pair! []
  (compatibility-pairs
   (mod (swap! compatibility-index inc) (count compatibility-pairs))))

(defn compatibility-probe! [test node]
  (let [[holder contender :as pair] (next-compatibility-pair!)]
    (with-open [a (pg/open test node)
                b (pg/open test node)]
      (jdbc/execute! a ["SELECT pg_advisory_lock(730203)"])
      (try
        (.setAutoCommit ^Connection a false)
        (.setAutoCommit ^Connection b false)
        (lock-row! a "o_lock_accounts" 24 holder)
        (let [result (try
                       (lock-row! b "o_lock_accounts" 24 contender true)
                       :acquired
                       (catch Throwable error
                         (if (lock-unavailable? error)
                           :blocked
                           (throw error))))
              expected (if (compatibility pair) :acquired :blocked)]
          (when-not (= expected result)
            (throw (ex-info "row-lock compatibility mismatch"
                            {:holder holder :contender contender
                             :expected expected :actual result})))
          (when (= :blocked result)
            (bump! :expected-lock-conflicts))
          (swap! covered-pairs conj pair)
          (bump! :compatibility-probes)
          {:holder holder :contender contender :result result})
        (finally
          (rollback-quietly! b)
          (rollback-quietly! a)
          (jdbc/execute! a ["SELECT pg_advisory_unlock(730203)"]))))))

(def savepoint-scenarios
  [:release-inner :retain-outer :restore-weaker])

(defn next-savepoint-scenario! []
  (savepoint-scenarios
   (mod (swap! savepoint-index inc) (count savepoint-scenarios))))

(defn expect-nowait! [connection key mode expected]
  (let [actual (try
                 (lock-row! connection "o_lock_accounts" key mode true)
                 :acquired
                 (catch Throwable error
                   (if (lock-unavailable? error)
                     :blocked
                     (throw error))))]
    (when-not (= expected actual)
      (throw (ex-info "savepoint lock state mismatch"
                      {:key key :mode mode :expected expected :actual actual})))
    actual))

(defn savepoint-probe! [test node]
  (let [scenario (next-savepoint-scenario!)]
    (with-open [a (pg/open test node)
                b (pg/open test node)]
      (jdbc/execute! a ["SELECT pg_advisory_lock(730204)"])
      (try
        (.setAutoCommit ^Connection a false)
        (.setAutoCommit ^Connection b false)
        (case scenario
          :release-inner
          (do
            (jdbc/execute! a ["SAVEPOINT lock_sp"])
            (lock-row! a "o_lock_accounts" 25 :update)
            (jdbc/execute! a ["ROLLBACK TO SAVEPOINT lock_sp"])
            (expect-nowait! b 25 :update :acquired))

          :retain-outer
          (do
            (lock-row! a "o_lock_accounts" 26 :update)
            (jdbc/execute! a ["SAVEPOINT lock_sp"])
            (lock-row! a "o_lock_accounts" 27 :update)
            (jdbc/execute! a ["ROLLBACK TO SAVEPOINT lock_sp"])
            (expect-nowait! b 26 :update :blocked)
            ;; A NOWAIT error aborts B's transaction.  Start a fresh one before
            ;; checking that the post-savepoint lock was released.
            (rollback-quietly! b)
            (expect-nowait! b 27 :update :acquired))

          :restore-weaker
          (do
            (lock-row! a "o_lock_accounts" 28 :key-share)
            (jdbc/execute! a ["SAVEPOINT lock_sp"])
            (lock-row! a "o_lock_accounts" 28 :update)
            (jdbc/execute! a ["ROLLBACK TO SAVEPOINT lock_sp"])
            (expect-nowait! b 28 :share :acquired)))
        (swap! covered-savepoints conj scenario)
        (bump! :savepoint-probes)
        (bump! :savepoint-rollbacks)
        {:scenario scenario}
        (finally
          (rollback-quietly! b)
          (rollback-quietly! a)
          (jdbc/execute! a ["SELECT pg_advisory_unlock(730204)"]))))))

(defn counter-operation! [connection process]
  (let [key (random/long 8)
        delta (inc (random/long 5))
        commit? (not (zero? (random/long 5)))]
    (let [auto-commit (.getAutoCommit ^Connection connection)]
      (try
        (.setAutoCommit ^Connection connection false)
        (jdbc/execute! connection ["SET LOCAL lock_timeout = '10s'"])
        (lock-row! connection "o_lock_accounts" key :update)
        (let [{:keys [balance]} (execute-one!
                                 connection
                                 ["SELECT balance FROM o_lock_accounts WHERE id = ?" key])
              op-id (:op_id (execute-one!
                             connection
                             ["SELECT nextval('lock_operation_id') AS op_id"]))]
          (when (zero? (random/long 4))
            (jdbc/execute! connection ["SAVEPOINT counter_sp"])
            (jdbc/execute! connection
                           ["UPDATE o_lock_accounts SET balance = balance + 1000000 WHERE id = ?"
                            key])
            (jdbc/execute! connection ["ROLLBACK TO SAVEPOINT counter_sp"])
            (bump! :savepoint-rollbacks))
          (jdbc/execute! connection
                         ["UPDATE o_lock_accounts
                             SET balance = ?, version = version + 1,
                                 payload = repeat(md5(?::text), 8)
                           WHERE id = ?"
                          (+ balance delta) op-id key])
          (jdbc/execute! connection
                         ["INSERT INTO h_lock_ledger
                            (op_id, account_id, balance_delta, version_delta, worker)
                           VALUES (?, ?, ?, 1, ?)"
                          op-id key delta process])
          (if commit?
            (do
              (.commit ^Connection connection)
              (bump! :counter-writes))
            (do
              (.rollback ^Connection connection)
              (bump! :whole-rollbacks)))
          {:key key :delta delta :committed commit? :op-id op-id})
        (catch Throwable error
          (rollback-quietly! connection)
          (throw error))
        (finally
          (try (.setAutoCommit ^Connection connection auto-commit)
               (catch Throwable _)))))))

(defn lock-only-operation! [connection]
  (let [key (random/long 8)
        mode (lock-modes (random/long (count lock-modes)))]
    (with-transaction!
     connection
     (fn [tx]
       (lock-row! tx "o_lock_accounts" key mode)
       (bump! :lock-only-transactions)
       {:key key :mode mode}))))

(defn item-operation! [connection]
  (let [key (random/long 16)]
    (with-transaction!
     connection
     (fn [tx]
       (let [{:keys [sequence_no]}
             (execute-one! tx
                           ["UPDATE o_lock_guards SET sequence_no = sequence_no + 1
                               WHERE id = ? RETURNING sequence_no" key])
             delete? (zero? (random/long 4))
             value (str "item-" key "-" sequence_no)]
         (if delete?
           (jdbc/execute! tx ["DELETE FROM o_lock_items WHERE id = ?" key])
           (jdbc/execute! tx
                          ["INSERT INTO o_lock_items(id, value, version)
                             VALUES (?, ?, ?)
                             ON CONFLICT (id) DO UPDATE
                             SET value = EXCLUDED.value, version = EXCLUDED.version"
                           key value sequence_no]))
         (jdbc/execute! tx
                        ["INSERT INTO h_lock_item_journal
                           (item_id, sequence_no, present, value)
                          VALUES (?, ?, ?, ?)"
                         key sequence_no (not delete?) (when-not delete? value)])
         (bump! :item-writes)
         {:key key :sequence sequence_no :deleted delete?})))))

(defn fk-operation! [connection]
  (let [parent (random/long 8)
        child-id (:op_id (execute-one!
                          connection
                          ["SELECT nextval('lock_operation_id') AS op_id"]))]
    (with-transaction!
     connection
     (fn [tx]
       (jdbc/execute! tx
                      ["UPDATE o_lock_parents SET payload = payload + 1 WHERE id = ?"
                       parent])
       (jdbc/execute! tx
                      ["INSERT INTO o_lock_children(id, parent_id) VALUES (?, ?)"
                       child-id parent])
       (when (zero? (random/long 2))
         (jdbc/execute! tx ["DELETE FROM o_lock_children WHERE id = ?" child-id]))
       (bump! :fk-writes)
       {:parent parent :child child-id}))))

(defn role [process]
  (case (mod process 6)
    0 :compatibility
    1 :savepoint
    2 :counter
    3 :item
    4 :foreign-key
    5 :lock-only))

(defn worker-role [process]
  (if-let [worker (some->> (.getName (Thread/currentThread))
                           (re-find #"worker ([0-9]+)$")
                           second
                           parse-long)]
    (role worker)
    (role process)))

(defn summary [provisional]
  (assoc @metrics
         :valid (zero? (:operation-failures @metrics))
         :operations @operation-count
         :covered-modes (sort @covered-modes)
         :covered-pairs (count @covered-pairs)
         :covered-savepoints (sort @covered-savepoints)
         :provisional provisional
         :updated-at (str (Instant/now))))

(defn persist-progress! [& [force?]]
  (let [operations (swap! operation-count inc)]
    (when (or force? (= operations 1) (zero? (mod operations 50)))
      (atomic-json! "row-lock-driver-summary.json" (summary true)))))

(defrecord RowLockClient [node connection]
  client/Client
  (open! [this _ node]
    (assoc this :node node :connection (atom nil)))

  (setup! [_ test]
    (with-open [conn (open-retrying (assoc test :node node) 60)]
      (let [row (execute-one! conn
                              ["SELECT version FROM row_lock_workload_setup WHERE id = 1"])]
        (when-not (= schema-version (:version row))
          (throw (ex-info "row-lock workload schema is not initialized" row))))))

  (invoke! [_ test op]
    (try
      (let [role (worker-role (:process op))
            value (if (#{:compatibility :savepoint} role)
                    (case role
                      :compatibility (compatibility-probe! test node)
                      :savepoint (savepoint-probe! test node))
                    (let [conn (or @connection
                                   (let [opened (pg/open test node)]
                                     (reset! connection opened)
                                     opened))]
                      (case role
                        :counter (counter-operation! conn (:process op))
                        :item (item-operation! conn)
                        :foreign-key (fk-operation! conn)
                        :lock-only (lock-only-operation! conn))))]
        (persist-progress!)
        (assoc op :type :ok :value (assoc value :role role)))
      (catch Throwable error
        (if (retryable? error)
          (do
            (close-quietly! @connection)
            (reset! connection nil)
            (bump! :operation-failures)
            (persist-progress! true)
            (assoc op :type :fail :error :unexpected-connection-loss
                   :value {:message (.getMessage error)}))
          (do
            (bump! :operation-failures)
            (persist-progress! true)
            (assoc op :type :fail :error :row-lock-failure
                   :value {:message (.getMessage error)
                           :sql-state (sql-state error)
                           :data (ex-data error)}))))))

  (teardown! [_ _])

  (close! [_ _]
    (close-quietly! @connection)))

(defn workload-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [failures (filter #(= :fail (:type %)) history)
            result (assoc (summary false)
                          :valid (empty? failures)
                          :failure-count (count failures)
                          :failures (take 10 failures))]
        (atomic-json! "row-lock-driver-summary.json" result)
        {:valid? (empty? failures) :summary result}))))

(defn workload [_]
  {:client (RowLockClient. nil nil)
   :checker (workload-checker)
   :generator (repeat {:type :invoke :f :tick})})

(defn table-am []
  (let [value (or (System/getenv "ROW_LOCK_TABLE_AM") "orioledb")]
    (when-not (#{"orioledb" "heap"} value)
      (throw (ex-info "ROW_LOCK_TABLE_AM must be orioledb or heap" {:value value})))
    value))

(defn schema-statements [am]
  [(str "CREATE TABLE IF NOT EXISTS o_lock_accounts
          (id bigint PRIMARY KEY, balance bigint NOT NULL, version bigint NOT NULL,
           unique_value bigint UNIQUE NOT NULL, payload text) USING " am)
   "CREATE TABLE IF NOT EXISTS h_lock_initial
      (id bigint PRIMARY KEY, balance bigint NOT NULL, version bigint NOT NULL) USING heap"
   "CREATE TABLE IF NOT EXISTS h_lock_ledger
      (op_id bigint PRIMARY KEY, account_id bigint NOT NULL, balance_delta bigint NOT NULL,
       version_delta bigint NOT NULL, worker bigint NOT NULL,
       committed_at timestamptz NOT NULL DEFAULT clock_timestamp()) USING heap"
   (str "CREATE TABLE IF NOT EXISTS o_lock_guards
          (id bigint PRIMARY KEY, sequence_no bigint NOT NULL) USING " am)
   (str "CREATE TABLE IF NOT EXISTS o_lock_items
          (id bigint PRIMARY KEY, value text NOT NULL, version bigint NOT NULL) USING " am)
   "CREATE TABLE IF NOT EXISTS h_lock_item_journal
      (item_id bigint NOT NULL, sequence_no bigint NOT NULL, present boolean NOT NULL,
       value text, PRIMARY KEY(item_id, sequence_no)) USING heap"
   (str "CREATE TABLE IF NOT EXISTS o_lock_parents
          (id bigint PRIMARY KEY, payload bigint NOT NULL) USING " am)
   (str "CREATE TABLE IF NOT EXISTS o_lock_children
          (id bigint PRIMARY KEY, parent_id bigint NOT NULL REFERENCES o_lock_parents(id))
          USING " am)])

(defn initialize! []
  (let [test (environment-test)
        am (table-am)]
    (with-open [connection (open-retrying test)]
      (jdbc/execute! connection ["SELECT pg_advisory_lock(730202)"])
      (try
        (jdbc/execute! connection ["CREATE EXTENSION IF NOT EXISTS orioledb"])
        (jdbc/execute! connection ["CREATE SEQUENCE IF NOT EXISTS lock_operation_id"])
        (jdbc/execute! connection
                       ["CREATE TABLE IF NOT EXISTS row_lock_workload_setup
                          (id integer PRIMARY KEY, version integer NOT NULL,
                           table_am text NOT NULL, initialized_at timestamptz NOT NULL) USING heap"])
        (doseq [statement (schema-statements am)]
          (jdbc/execute! connection [statement]))
        (let [setup (execute-one! connection
                                  ["SELECT version, table_am FROM row_lock_workload_setup WHERE id = 1"])]
          (when (and setup (not= am (:table_am setup)))
            (throw (ex-info "existing row-lock schema uses another table AM"
                            {:expected am :actual (:table_am setup)})))
          (when-not (= schema-version (:version setup))
            (with-transaction!
             connection
             (fn [tx]
               (jdbc/execute!
                tx
                ["TRUNCATE o_lock_children, o_lock_parents,
                           h_lock_item_journal, o_lock_items, o_lock_guards,
                           h_lock_ledger, h_lock_initial, o_lock_accounts"])
               (jdbc/execute! tx
                              ["INSERT INTO o_lock_accounts
                                 SELECT i, 0, 0, 100000 + i, repeat(md5(i::text), 8)
                                 FROM generate_series(0, 31) i"])
               (jdbc/execute! tx
                              ["INSERT INTO h_lock_initial
                                 SELECT id, balance, version FROM o_lock_accounts"])
               (jdbc/execute! tx
                              ["INSERT INTO o_lock_guards SELECT i, 0
                                 FROM generate_series(0, 15) i"])
               (jdbc/execute! tx
                              ["INSERT INTO o_lock_items SELECT i, 'initial-' || i, 0
                                 FROM generate_series(0, 15) i"])
               (jdbc/execute! tx
                              ["INSERT INTO h_lock_item_journal
                                 SELECT i, 0, true, 'initial-' || i
                                 FROM generate_series(0, 15) i"])
               (jdbc/execute! tx
                              ["INSERT INTO o_lock_parents SELECT i, 0
                                 FROM generate_series(0, 7) i"])
               (jdbc/execute! tx
                              ["INSERT INTO row_lock_workload_setup
                                 VALUES (1, ?, ?, clock_timestamp())
                                 ON CONFLICT (id) DO UPDATE SET
                                   version = EXCLUDED.version,
                                   table_am = EXCLUDED.table_am,
                                   initialized_at = EXCLUDED.initialized_at"
                               schema-version am])))))
        (spit (io/file state-dir "row-lock-setup.ready")
              (json/generate-string {:schema-version schema-version
                                     :table-am am
                                     :ready-at (str (Instant/now))}))
        (finally
          (jdbc/execute! connection ["SELECT pg_advisory_unlock(730202)"]))))))

(def ledger-mismatch-sql
  "SELECT count(*) AS count
     FROM o_lock_accounts a
     JOIN h_lock_initial i USING (id)
     LEFT JOIN (
       SELECT account_id, sum(balance_delta) AS balance_delta,
              sum(version_delta) AS version_delta
         FROM h_lock_ledger GROUP BY account_id
     ) l ON l.account_id = a.id
    WHERE a.balance <> i.balance + coalesce(l.balance_delta, 0)
       OR a.version <> i.version + coalesce(l.version_delta, 0)")

(def journal-mismatch-sql
  "WITH latest AS (
     SELECT DISTINCT ON (item_id) item_id, present, value
       FROM h_lock_item_journal ORDER BY item_id, sequence_no DESC
   )
   SELECT count(*) AS count
     FROM latest l FULL JOIN o_lock_items i ON i.id = l.item_id
    WHERE (l.present AND i.id IS NULL)
       OR (NOT l.present AND i.id IS NOT NULL)
       OR (l.present AND i.value IS DISTINCT FROM l.value)")

(defn read-json [name]
  (let [file (io/file state-dir name)]
    (when (.isFile file)
      (json/parse-string (slurp file) true))))

(defn structural-verification! [connection am]
  (when (= "orioledb" am)
    (doseq [table ["o_lock_accounts" "o_lock_guards" "o_lock_items"
                   "o_lock_parents" "o_lock_children"]]
      (let [rows (jdbc/execute! connection
                                [(str "SELECT * FROM verify_orioledb('" table
                                      "'::regclass, true)")]
                                builder)]
        (when (seq rows)
          (throw (ex-info "verify_orioledb reported corruption"
                          {:table table :rows (take 10 rows)}))))))
  true)

(defn final-verification! []
  (let [test (environment-test)
        driver (read-json "row-lock-driver-summary.json")
        am (table-am)]
    (with-open [connection (open-retrying test 300)]
      (let [ledger-mismatches (:count (execute-one! connection [ledger-mismatch-sql]))
            journal-mismatches (:count (execute-one! connection [journal-mismatch-sql]))
            orphans (:count
                     (execute-one!
                      connection
                      ["SELECT count(*) AS count FROM o_lock_children c
                         LEFT JOIN o_lock_parents p ON p.id = c.parent_id
                        WHERE p.id IS NULL"]))
            structure-valid (structural-verification! connection am)
            summary {:valid (and driver (:valid driver)
                                  (zero? ledger-mismatches)
                                  (zero? journal-mismatches)
                                  (zero? orphans)
                                  structure-valid)
                     :driver driver
                     :table-am am
                     :ledger-mismatches ledger-mismatches
                     :journal-mismatches journal-mismatches
                     :foreign-key-orphans orphans
                     :structural-verification structure-valid
                     :finished-at (str (Instant/now))}]
        (atomic-json! "row-lock-final-summary.json" summary)
        summary))))

(defn -main [& [mode]]
  (case mode
    "initialize" (initialize!)
    "final" (final-verification!)
    (throw (ex-info "usage: row-locks initialize|final" {:mode mode}))))
