(ns jepsen.orioledb.workload.checkpoint-recovery
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.postgres.client :as pg]
            [jepsen.random :as random]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.math BigInteger)
           (java.nio.file Files StandardCopyOption)
           (java.security MessageDigest)
           (java.sql Connection SQLException)
           (java.time Instant)))

(def schema-version 1)
(def state-dir (or (System/getenv "RUNTIME_STATE_DIR") "/runtime-state"))
(def builder {:builder-fn rs/as-unqualified-lower-maps})

(def initial-metrics
  {:append-writes 0
   :churn-writes 0
   :toast-writes 0
   :ddl-operations 0
   :checkpoints-started 0
   :checkpoints-completed 0
   :checkpoint-spanning-transactions 0
   :checkpoint-spanning-commits 0
   :checkpoint-spanning-rollbacks 0
   :verifications 0
   :structural-verifications 0
   :recovery-probes 0
   :generation-changes 0
   :interrupted-checkpoints 0
   :repeated-boots-before-ready 0
   :operation-failures 0})

(defonce metrics (atom initial-metrics))
(defonce claimed-generations (atom #{}))
(defonce operation-count (atom 0))

(defn bump! [k]
  (swap! metrics update k (fnil inc 0)))

(defn md5 [parts]
  (let [digest (MessageDigest/getInstance "MD5")
        value (str/join \u001f (map #(if (nil? %) "<null>" (str %)) parts))]
    (format "%032x" (BigInteger. 1 (.digest digest (.getBytes value "UTF-8"))))))

(defn retryable? [^Throwable error]
  (or (and (instance? SQLException error)
           (let [state (.getSQLState ^SQLException error)]
             (or (and state (str/starts-with? state "08"))
                 (#{"57P01" "57P02" "57P03"} state))))
      (some? (re-find #"(?i)connection.*(closed|refused|reset)|broken pipe|socket|I/O error"
                      (or (.getMessage error) "")))))

(defn serialization-failure? [^Throwable error]
  (and (instance? SQLException error)
       (= "40001" (.getSQLState ^SQLException error))))

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

(defn in-transaction! [^Connection connection f]
  (let [auto-commit (.getAutoCommit connection)]
    (try
      (.setAutoCommit connection false)
      (let [result (f connection)]
        (.commit connection)
        result)
      (catch Throwable error
        (try (.rollback connection) (catch Throwable _))
        (throw error))
      (finally
        (try (.setAutoCommit connection auto-commit) (catch Throwable _))))))

(defn generation [connection]
  (execute-one!
   connection
   ["SELECT current_setting('cluster_name') AS boot_id,
            pg_postmaster_start_time()::text AS postmaster_start_time"]))

(def pair-difference-sql
  "SELECT
     (SELECT count(*) FROM
       ((SELECT op_id, tx_id, payload, checksum FROM o_append
         EXCEPT SELECT op_id, tx_id, payload, checksum FROM h_append)
        UNION ALL
        (SELECT op_id, tx_id, payload, checksum FROM h_append
         EXCEPT SELECT op_id, tx_id, payload, checksum FROM o_append)) d) AS append_diff,
     (SELECT count(*) FROM
       ((SELECT id, version, secondary_key, nullable_value, payload, checksum FROM o_churn
         EXCEPT SELECT id, version, secondary_key, nullable_value, payload, checksum FROM h_churn)
        UNION ALL
        (SELECT id, version, secondary_key, nullable_value, payload, checksum FROM h_churn
         EXCEPT SELECT id, version, secondary_key, nullable_value, payload, checksum FROM o_churn)) d) AS churn_diff,
     (SELECT count(*) FROM
       ((SELECT id, version, profile, payload, checksum FROM o_toast
         EXCEPT SELECT id, version, profile, payload, checksum FROM h_toast)
        UNION ALL
        (SELECT id, version, profile, payload, checksum FROM h_toast
         EXCEPT SELECT id, version, profile, payload, checksum FROM o_toast)) d) AS toast_diff")

(def checksum-sql
  "SELECT
     (SELECT count(*) FROM o_append WHERE checksum <> workload_checksum(op_id::text, tx_id::text, payload)) AS append_bad,
     (SELECT count(*) FROM o_churn WHERE checksum <> workload_checksum(id::text, version::text, secondary_key::text, nullable_value, payload)) AS churn_bad,
     (SELECT count(*) FROM o_toast WHERE checksum <> workload_checksum(id::text, version::text, profile, payload)) AS toast_bad")

(defn zero-values? [row]
  (every? zero? (map long (vals row))))

(def index-probes
  [{:index "o_append_pkey"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT op_id FROM o_append WHERE op_id = 1"}
   {:index "o_append_tx_idx"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT op_id FROM o_append WHERE tx_id BETWEEN 1 AND 10"}
   {:index "o_churn_pkey"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT id FROM o_churn WHERE id = 1"}
   {:index "o_churn_secondary_idx"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT id FROM o_churn WHERE secondary_key BETWEEN 1 AND 10"}
   {:index "o_toast_pkey"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT id FROM o_toast WHERE id = 1"}
   {:index "o_toast_profile_idx"
    :sql "EXPLAIN (FORMAT JSON, COSTS OFF) SELECT id FROM o_toast WHERE profile = 'small'"}])

(defn verify-indexes! [connection]
  (jdbc/execute! connection ["SET LOCAL enable_seqscan = off"])
  (doseq [{:keys [index sql]} index-probes]
    (let [plan (pr-str (jdbc/execute! connection [sql] builder))]
      (when-not (str/includes? plan index)
        (throw (ex-info "expected native index was not selected"
                        {:index index :plan plan}))))))

(def ddl-state-expectations
  {0 {:scratch? false :column nil :scratch-index? false :churn-index? false}
   1 {:scratch? true :column nil :scratch-index? false :churn-index? false}
   2 {:scratch? true :column "added_value" :scratch-index? false :churn-index? false}
   3 {:scratch? true :column "renamed_value" :scratch-index? false :churn-index? false}
   4 {:scratch? true :column "renamed_value" :scratch-index? true :churn-index? false}
   5 {:scratch? true :column "renamed_value" :scratch-index? false :churn-index? false}
   6 {:scratch? true :column "renamed_value" :scratch-index? false :churn-index? false :truncated? true}
   7 {:scratch? true :column nil :scratch-index? false :churn-index? false :truncated? true}
   8 {:scratch? false :column nil :scratch-index? false :churn-index? true}})

(defn expected-ddl-state [phase]
  (or (ddl-state-expectations phase)
      (throw (ex-info "unknown DDL phase" {:phase phase}))))

(defn relation-exists? [connection relation]
  (some? (:relation
          (execute-one! connection
                        ["SELECT to_regclass(?)::text AS relation" relation]))))

(defn column-exists? [connection relation column]
  (:exists
   (execute-one!
    connection
    ["SELECT EXISTS (
        SELECT 1 FROM pg_attribute
         WHERE attrelid = to_regclass(?) AND attname = ? AND NOT attisdropped
      ) AS exists"
     relation column])))

(defn verify-ddl-index-results! [connection]
  (jdbc/execute! connection ["SET LOCAL enable_seqscan = off"])
  (let [plan (pr-str (jdbc/execute!
                      connection
                      ["EXPLAIN (FORMAT JSON, COSTS OFF)
                        SELECT id FROM o_churn WHERE nullable_value = 'value-1'"]
                      builder))
        differences
        (:count
         (execute-one!
          connection
          ["SELECT count(*) AS count FROM
              ((SELECT id FROM o_churn WHERE nullable_value IS NULL
                EXCEPT SELECT id FROM h_churn WHERE nullable_value IS NULL)
               UNION ALL
               (SELECT id FROM h_churn WHERE nullable_value IS NULL
                EXCEPT SELECT id FROM o_churn WHERE nullable_value IS NULL)) d"]))]
    (when-not (str/includes? plan "o_churn_ddl_nullable_idx")
      (throw (ex-info "DDL-created native index was not selected" {:plan plan})))
    (when-not (zero? differences)
      (throw (ex-info "DDL-created index returned inconsistent rows"
                      {:differences differences})))))

(defn verify-ddl-state! [connection]
  (in-transaction!
   connection
   (fn [tx]
     (jdbc/execute! tx ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"])
     (let [{:keys [phase epoch]} (execute-one!
                                  tx
                                  ["SELECT phase, epoch FROM ddl_state
                                    WHERE id = 1 FOR SHARE"])
           expected (expected-ddl-state phase)
           actual {:scratch? (relation-exists? tx "o_ddl_scratch")
                   :column (cond
                             (column-exists? tx "o_ddl_scratch" "added_value") "added_value"
                             (column-exists? tx "o_ddl_scratch" "renamed_value") "renamed_value"
                             :else nil)
                   :scratch-index? (relation-exists? tx "o_ddl_scratch_payload_idx")
                   :churn-index? (and (relation-exists? tx "o_churn_ddl_nullable_idx")
                                      (relation-exists? tx "h_churn_ddl_nullable_idx"))}]
       (when-not (= (select-keys expected [:scratch? :column :scratch-index? :churn-index?])
                    actual)
         (throw (ex-info "transactional DDL state disagrees with its durable journal"
                         {:phase phase :epoch epoch :expected expected :actual actual})))
       (when (:truncated? expected)
         (let [row (execute-one! tx
                                 ["SELECT count(*) AS count, min(id) AS id FROM o_ddl_scratch"])]
           (when-not (= {:count 1 :id 700} row)
             (throw (ex-info "DDL scratch table contents are inconsistent after TRUNCATE"
                             {:phase phase :row row})))))
       (when (and (:scratch? expected) (not (:truncated? expected)))
         (let [column (:column expected)
               row (execute-one!
                    tx
                    [(str "SELECT count(*) AS count, min(id) AS id, min(payload) AS payload"
                          (when column (str ", min(" column ") AS column_value"))
                          " FROM o_ddl_scratch")])
               expected-row (cond-> {:count 1 :id 1 :payload "created"}
                              column (assoc :column_value 17))]
           (when-not (= expected-row row)
             (throw (ex-info "DDL scratch table contents are inconsistent"
                             {:phase phase :expected expected-row :row row})))))
       (when (:churn-index? expected)
         (verify-ddl-index-results! tx))
       {:phase phase :epoch epoch}))))

(defn verify-ddl-state-retrying! [connection]
  (loop [remaining 20]
    (let [result (try
                   {:value (verify-ddl-state! connection)}
                   (catch Throwable error {:error error}))]
      (if-let [error (:error result)]
        (if (and (serialization-failure? error) (pos? remaining))
          (do
            (Thread/sleep 10)
            (recur (dec remaining)))
          (throw error))
        (:value result)))))

(defn verify-data! [connection]
  (let [result
        (in-transaction!
         connection
         (fn [tx]
           (jdbc/execute! tx ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"])
           (jdbc/execute! tx ["SET LOCAL statement_timeout = '60s'"])
           (jdbc/execute! tx ["SET LOCAL lock_timeout = '10s'"])
           (let [differences (execute-one! tx [pair-difference-sql])
                 bad-checksums (execute-one! tx [checksum-sql])]
             (when-not (zero-values? differences)
               (throw (ex-info "target/shadow mismatch" differences)))
             (when-not (zero-values? bad-checksums)
               (throw (ex-info "deterministic checksum mismatch" bad-checksums)))
             (verify-indexes! tx)
             {:target-shadow-equal true
              :checksums-valid true
              :index-probes-valid true
              :differences differences
              :bad-checksums bad-checksums})))]
    (verify-ddl-state-retrying! connection)
    (bump! :verifications)
    result))

(defn append! [connection process]
  (in-transaction!
   connection
   (fn [tx]
     (let [op-id (:op_id (execute-one! tx ["SELECT nextval('append_operation_id') AS op_id"]))
           payload (str "append:" op-id ":" (apply str (repeat 64 (char (+ 65 (mod op-id 26))))))
           checksum (md5 [op-id process payload])
           values [op-id process payload checksum]]
       (jdbc/execute! tx (into ["INSERT INTO o_append VALUES (?, ?, ?, ?)"] values))
       (jdbc/execute! tx (into ["INSERT INTO h_append VALUES (?, ?, ?, ?)"] values))
       (bump! :append-writes)
       {:op-id op-id}))))

(defn churn! [connection process]
  (let [key (+ (* (max 0 (- process 2)) 512) (random/long 512))
        version (inc (random/long 1000000))
        secondary-key (random/long 256)
        nullable-value (when (random/bool) (str "value-" version))
        payload (if (random/bool)
                  (apply str (repeat 64 (md5 [key version])))
                  (str "inline-" key "-" version))
        checksum (md5 [key version secondary-key nullable-value payload])]
    (in-transaction!
     connection
     (fn [tx]
       (if (zero? (random/long 8))
         (do
           (jdbc/execute! tx ["DELETE FROM o_churn WHERE id = ?" key])
           (jdbc/execute! tx ["DELETE FROM h_churn WHERE id = ?" key]))
         (doseq [table ["o_churn" "h_churn"]]
           (jdbc/execute!
            tx
            [(str "INSERT INTO " table " VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (id) DO UPDATE SET
                     version = EXCLUDED.version,
                     secondary_key = EXCLUDED.secondary_key,
                     nullable_value = EXCLUDED.nullable_value,
                     payload = EXCLUDED.payload,
                     checksum = EXCLUDED.checksum")
             key version secondary-key nullable-value payload checksum])))
       (bump! :churn-writes)
       {:key key :version version}))))

(defn toast-payload [profile key version]
  (case profile
    "null" nil
    "small" (str "small-" key "-" version)
    "repeated" (apply str (repeat 1024 (str "toast-" key "-")))
    "incompressible" (apply str (map #(md5 [key version %]) (range 256)))))

(defn toast! [connection]
  (let [key (random/long 4096)
        version (inc (random/long 1000000))
        profile (["null" "small" "repeated" "incompressible"] (random/long 4))
        payload (toast-payload profile key version)
        checksum (md5 [key version profile payload])]
    (in-transaction!
     connection
     (fn [tx]
       (doseq [table ["o_toast" "h_toast"]]
         (jdbc/execute!
          tx
          [(str "INSERT INTO " table " VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (id) DO UPDATE SET
                   version = EXCLUDED.version,
                   profile = EXCLUDED.profile,
                   payload = EXCLUDED.payload,
                   checksum = EXCLUDED.checksum")
           key version profile payload checksum]))
       (bump! :toast-writes)
       {:key key :version version :profile profile}))))

(def ddl-transitions
  {0 {:next 1
      :operation "create-scratch-table"
      :statements ["CREATE TABLE o_ddl_scratch
                      (id integer PRIMARY KEY, payload text NOT NULL)
                    USING orioledb"
                   "INSERT INTO o_ddl_scratch VALUES (1, 'created')"]}
   1 {:next 2
      :operation "add-column"
      :statements ["ALTER TABLE o_ddl_scratch
                      ADD COLUMN added_value integer NOT NULL DEFAULT 17"]}
   2 {:next 3
      :operation "rename-column"
      :statements ["ALTER TABLE o_ddl_scratch
                      RENAME COLUMN added_value TO renamed_value"]}
   3 {:next 4
      :operation "create-scratch-index"
      :statements ["CREATE INDEX o_ddl_scratch_payload_idx
                      ON o_ddl_scratch(payload) WITH (compress = 5)"]}
   4 {:next 5
      :operation "drop-scratch-index"
      :statements ["DROP INDEX o_ddl_scratch_payload_idx"]}
   5 {:next 6
      :operation "truncate-scratch-table"
      :statements ["TRUNCATE o_ddl_scratch"
                   "INSERT INTO o_ddl_scratch(id, payload, renamed_value)
                      VALUES (700, 'after-truncate', 19)"]}
   6 {:next 7
      :operation "drop-column"
      :statements ["ALTER TABLE o_ddl_scratch DROP COLUMN renamed_value"]}
   7 {:next 8
      :operation "replace-table-with-live-indexes"
      :statements ["DROP TABLE o_ddl_scratch"
                   "CREATE INDEX o_churn_ddl_nullable_idx
                      ON o_churn(nullable_value) WITH (compress = 5)"
                   "CREATE INDEX h_churn_ddl_nullable_idx
                      ON h_churn(nullable_value)"]}
   8 {:next 0
      :operation "drop-live-indexes"
      :statements ["DROP INDEX o_churn_ddl_nullable_idx"
                   "DROP INDEX h_churn_ddl_nullable_idx"]}})

(defn ddl! [connection]
  (let [{:keys [boot_id]} (generation connection)
        operation
        (in-transaction!
         connection
         (fn [tx]
           (let [{:keys [phase epoch]} (execute-one!
                                        tx
                                        ["SELECT phase, epoch FROM ddl_state
                                          WHERE id = 1 FOR UPDATE"])
                 {:keys [next operation statements]} (ddl-transitions phase)]
             (doseq [statement statements]
               (jdbc/execute! tx [statement]))
             (jdbc/execute! tx
                            ["UPDATE ddl_state SET epoch = ?, phase = ? WHERE id = 1"
                             (inc epoch) next])
             (jdbc/execute! tx
                            ["INSERT INTO ddl_history(epoch, phase, operation, boot_id)
                              VALUES (?, ?, ?, ?)"
                             (inc epoch) next operation boot_id])
             operation)))]
    (let [verified (verify-ddl-state! connection)]
      (bump! :ddl-operations)
      (Thread/sleep 100)
      (assoc verified :operation operation))))

(defn checkpoint! [connection kind]
  (let [{:keys [boot_id postmaster_start_time]} (generation connection)
        attempt (:attempt_id
                 (execute-one!
                  connection
                  ["INSERT INTO checkpoint_attempts(kind, boot_id, postmaster_start_time)
                    VALUES (?, ?, ?::timestamptz) RETURNING attempt_id"
                   kind boot_id postmaster_start_time]))]
    (bump! :checkpoints-started)
    (jdbc/execute! connection ["CHECKPOINT"])
    (jdbc/execute! connection
                   ["UPDATE checkpoint_attempts
                       SET completed_at = clock_timestamp(), completed_boot_id = ?
                     WHERE attempt_id = ?"
                    boot_id attempt])
    (bump! :checkpoints-completed)
    {:attempt attempt :generation [boot_id postmaster_start_time]}))

(defn checkpoint-with-open-transaction! [checkpoint-connection transaction-connection process]
  (let [auto-commit (.getAutoCommit ^Connection transaction-connection)]
    (try
      (.setAutoCommit ^Connection transaction-connection false)
      (let [op-id (:op_id (execute-one!
                           transaction-connection
                           ["SELECT nextval('append_operation_id') AS op_id"]))
            initial-payload (str "checkpoint-open:" op-id ":initial")
            updated-payload (str "checkpoint-open:" op-id ":after-savepoint")
            initial-checksum (md5 [op-id process initial-payload])
            updated-checksum (md5 [op-id process updated-payload])
            initial-values [op-id process initial-payload initial-checksum]]
        (jdbc/execute! transaction-connection
                       (into ["INSERT INTO o_append VALUES (?, ?, ?, ?)"]
                             initial-values))
        (jdbc/execute! transaction-connection
                       (into ["INSERT INTO h_append VALUES (?, ?, ?, ?)"]
                             initial-values))
        (jdbc/execute! transaction-connection ["SAVEPOINT checkpoint_recovery_sp"])
        (doseq [table ["o_append" "h_append"]]
          (jdbc/execute! transaction-connection
                         [(str "UPDATE " table
                               " SET payload = ?, checksum = ? WHERE op_id = ?")
                          updated-payload updated-checksum op-id]))
        (let [savepoint-rolled-back? (random/bool)
              commit? (random/bool)]
          (when savepoint-rolled-back?
            (jdbc/execute! transaction-connection
                           ["ROLLBACK TO SAVEPOINT checkpoint_recovery_sp"]))
          (let [checkpoint (checkpoint! checkpoint-connection "steady-state")]
            (Thread/sleep (random/long 101))
            (if commit?
              (do
                (.commit ^Connection transaction-connection)
                (bump! :checkpoint-spanning-commits))
              (do
                (.rollback ^Connection transaction-connection)
                (bump! :checkpoint-spanning-rollbacks)))
            (bump! :checkpoint-spanning-transactions)
            (assoc checkpoint
                   :open-transaction {:op-id op-id
                                      :outcome (if commit? :committed :rolled-back)
                                      :savepoint-rolled-back savepoint-rolled-back?}))))
      (catch Throwable error
        (try (.rollback ^Connection transaction-connection) (catch Throwable _))
        (throw error))
      (finally
        (try
          (.setAutoCommit ^Connection transaction-connection auto-commit)
          (catch Throwable _))))))

(declare structural-verification!)

(defn boot-number [boot-id]
  (some->> boot-id (re-find #"([0-9]+)$") second parse-long))

(defn recovery-probe! [connection]
  (let [{:keys [boot_id postmaster_start_time] :as current} (generation connection)
        generation-key [boot_id postmaster_start_time]]
    (when-not (contains? @claimed-generations generation-key)
      (let [previous (execute-one!
                      connection
                      ["SELECT boot_id FROM recovery_probes
                        ORDER BY probe_id DESC LIMIT 1"])
            previous-number (boot-number (:boot_id previous))
            current-number (boot-number boot_id)]
        (when previous
          (bump! :generation-changes)
          (when (and previous-number current-number
                     (> (- current-number previous-number) 1))
            (bump! :repeated-boots-before-ready)))
        (in-transaction!
         connection
         (fn [tx]
           (jdbc/execute! tx ["INSERT INTO recovery_write_probe(note) VALUES ('rollback')"])
           (.rollback ^Connection tx)))
        (try
          (structural-verification! connection)
          (verify-data! connection)
          (catch Throwable error
            ;; Retry connection interruptions, but report a safety failure only
            ;; once for a generation whose recovered state was readable.
            (when-not (retryable? error)
              (swap! claimed-generations conj generation-key))
            (throw error)))
        (let [interrupted (:count
                           (execute-one!
                            connection
                            ["SELECT count(*) AS count FROM checkpoint_attempts
                              WHERE completed_at IS NULL AND boot_id <> ?"
                             boot_id]))]
          (swap! metrics assoc :interrupted-checkpoints interrupted))
        (jdbc/execute! connection
                       ["INSERT INTO recovery_probes(boot_id, postmaster_start_time)
                         VALUES (?, ?::timestamptz)"
                        boot_id postmaster_start_time])
        (bump! :recovery-probes)
        (checkpoint! connection "post-recovery")
        (swap! claimed-generations conj generation-key)
        current))))

(defn role [process]
  (case process
    0 :checkpoint
    1 :recovery
    2 :ddl
    (case (mod (- process 3) 3)
      0 :append
      1 :churn
      2 :toast)))

(defn worker-role [process]
  (if-let [worker (some->> (.getName (Thread/currentThread))
                           (re-find #"worker ([0-9]+)$")
                           second
                           parse-long)]
    (role worker)
    (role process)))

(defn persist-progress!
  ([] (persist-progress! false))
  ([force?]
   (let [operations (swap! operation-count inc)]
    (when (or force? (= operations 1) (zero? (mod operations 50)))
      (atomic-json! "driver-summary.json"
                    (assoc @metrics
                           :valid (zero? (:operation-failures @metrics))
                           :operations operations
                           :provisional true
                           :updated-at (str (Instant/now))))))))

(defrecord RecoveryClient [node connection]
  client/Client
  (open! [this test node]
    (assoc this :node node :connection (atom nil)))

  (setup! [_ test]
    (with-open [conn (open-retrying (assoc test :node node) 60)]
      (let [row (execute-one! conn ["SELECT version FROM workload_setup WHERE id = 1"])]
        (when-not (= schema-version (:version row))
          (throw (ex-info "checkpoint recovery schema is not initialized" row))))))

  (invoke! [_ test op]
    (try
      (let [conn (or @connection
                     (let [opened (pg/open test node)]
                       (reset! connection opened)
                       opened))
            process (:process op)
            value (case (worker-role process)
                    :checkpoint (with-open [transaction-connection (pg/open test node)]
                                  (checkpoint-with-open-transaction!
                                   conn transaction-connection process))
                    :recovery (or (recovery-probe! conn) {:unchanged true})
                    :ddl (ddl! conn)
                    :append (append! conn process)
                    :churn (churn! conn process)
                    :toast (toast! conn))]
        (persist-progress!)
        (assoc op :type :ok :value value))
      (catch Throwable error
        (if (retryable? error)
          (do
            (close-quietly! @connection)
            (reset! connection nil)
            (assoc op :type :info
                   :error :connection-lost
                   :value {:message (.getMessage error)}))
          (do
            (bump! :operation-failures)
            (persist-progress! true)
            (assoc op :type :fail
                   :error :workload-failure
                   :value {:message (.getMessage error)
                           :data (ex-data error)}))))))

  (teardown! [_ _])

  (close! [_ _]
    (close-quietly! @connection)))

(defn workload-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [failures (filter #(= :fail (:type %)) history)
            summary (assoc @metrics
                           :valid (empty? failures)
                           :failure-count (count failures)
                           :operations @operation-count
                           :provisional false
                           :finished-at (str (Instant/now)))]
        (atomic-json! "driver-summary.json" summary)
        {:valid? (empty? failures)
         :summary summary
         :failures (take 10 failures)}))))

(defn workload [_]
  {:client (RecoveryClient. nil nil)
   :checker (workload-checker)
   :generator (repeat {:type :invoke :f :tick})})

(def schema-statements
  ["CREATE EXTENSION IF NOT EXISTS orioledb"
   "CREATE TABLE IF NOT EXISTS workload_setup
      (id integer PRIMARY KEY, version integer NOT NULL, initialized_at timestamptz NOT NULL,
       logical_bytes bigint NOT NULL, main_buffers_bytes bigint NOT NULL) USING heap"
   "CREATE SEQUENCE IF NOT EXISTS append_operation_id"
   "CREATE OR REPLACE FUNCTION workload_checksum(VARIADIC parts text[])
      RETURNS text LANGUAGE SQL IMMUTABLE PARALLEL SAFE
      AS $$ SELECT md5(array_to_string(parts, chr(31), '<null>')) $$"
   "CREATE TABLE IF NOT EXISTS o_append
      (op_id bigint PRIMARY KEY, tx_id bigint NOT NULL, payload text NOT NULL, checksum text NOT NULL)
      USING orioledb"
   "CREATE TABLE IF NOT EXISTS h_append
      (op_id bigint PRIMARY KEY, tx_id bigint NOT NULL, payload text NOT NULL, checksum text NOT NULL)
      USING heap"
   "CREATE INDEX IF NOT EXISTS o_append_tx_idx ON o_append(tx_id)"
   "CREATE INDEX IF NOT EXISTS h_append_tx_idx ON h_append(tx_id)"
   "CREATE TABLE IF NOT EXISTS o_churn
      (id integer PRIMARY KEY, version bigint NOT NULL, secondary_key integer NOT NULL,
       nullable_value text, payload text NOT NULL, checksum text NOT NULL)
      USING orioledb WITH (compress = 5, toast_compress = 5, primary_compress = 5)"
   "CREATE TABLE IF NOT EXISTS h_churn
      (id integer PRIMARY KEY, version bigint NOT NULL, secondary_key integer NOT NULL,
       nullable_value text, payload text NOT NULL, checksum text NOT NULL) USING heap"
   "CREATE INDEX IF NOT EXISTS o_churn_secondary_idx ON o_churn(secondary_key) WITH (compress = 5)"
   "CREATE INDEX IF NOT EXISTS h_churn_secondary_idx ON h_churn(secondary_key)"
   "CREATE TABLE IF NOT EXISTS o_toast
      (id integer PRIMARY KEY, version bigint NOT NULL, profile text NOT NULL,
       payload text, checksum text NOT NULL) USING orioledb"
   "CREATE TABLE IF NOT EXISTS h_toast
      (id integer PRIMARY KEY, version bigint NOT NULL, profile text NOT NULL,
       payload text, checksum text NOT NULL) USING heap"
   "CREATE INDEX IF NOT EXISTS o_toast_profile_idx ON o_toast(profile)"
   "CREATE INDEX IF NOT EXISTS h_toast_profile_idx ON h_toast(profile)"
   "CREATE TABLE IF NOT EXISTS checkpoint_attempts
      (attempt_id bigserial PRIMARY KEY, kind text NOT NULL, boot_id text NOT NULL,
       postmaster_start_time timestamptz NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
       completed_at timestamptz, completed_boot_id text) USING heap"
   "CREATE TABLE IF NOT EXISTS recovery_probes
      (probe_id bigserial PRIMARY KEY, boot_id text NOT NULL,
       postmaster_start_time timestamptz NOT NULL, verified_at timestamptz NOT NULL DEFAULT clock_timestamp()) USING heap"
   "CREATE TABLE IF NOT EXISTS verification_epochs
      (epoch_id bigserial PRIMARY KEY, boot_id text NOT NULL,
       verified_at timestamptz NOT NULL DEFAULT clock_timestamp(), final boolean NOT NULL) USING heap"
   "CREATE TABLE IF NOT EXISTS recovery_write_probe
      (id bigserial PRIMARY KEY, note text) USING heap"
   "CREATE TABLE IF NOT EXISTS ddl_state
      (id integer PRIMARY KEY, epoch bigint NOT NULL, phase integer NOT NULL) USING heap"
   "CREATE TABLE IF NOT EXISTS ddl_history
      (epoch bigint PRIMARY KEY, phase integer NOT NULL, operation text NOT NULL,
       boot_id text NOT NULL, completed_at timestamptz NOT NULL DEFAULT clock_timestamp()) USING heap"])

(defn initialize! []
  (let [test (environment-test)
        rows (parse-long (or (System/getenv "CHECKPOINT_RECOVERY_INITIAL_ROWS") "8192"))
        multiplier (parse-long (or (System/getenv "CHECKPOINT_RECOVERY_WORKING_SET_MULTIPLIER") "8"))]
    (with-open [connection (open-retrying test)]
      (jdbc/execute! connection ["SELECT pg_advisory_lock(730201)"])
      (try
        (doseq [statement schema-statements]
          (jdbc/execute! connection [statement]))
        (jdbc/execute! connection
                       ["INSERT INTO ddl_state VALUES (1, 0, 0)
                         ON CONFLICT (id) DO NOTHING"])
        (let [setup (execute-one! connection ["SELECT version FROM workload_setup WHERE id = 1"])]
          (when-not (= schema-version (:version setup))
            (in-transaction!
             connection
             (fn [tx]
               (doseq [table ["o_append" "h_append" "o_churn" "h_churn" "o_toast" "h_toast"]]
                 (jdbc/execute! tx [(str "TRUNCATE " table)]))
               (jdbc/execute! tx
                              ["INSERT INTO o_churn
                                SELECT g, 0, g % 256, CASE WHEN g % 3 = 0 THEN NULL ELSE 'initial-' || g END,
                                       repeat(md5(g::text), 128),
                                       workload_checksum(g::text, '0', (g % 256)::text,
                                         CASE WHEN g % 3 = 0 THEN NULL ELSE 'initial-' || g END,
                                         repeat(md5(g::text), 128))
                                  FROM generate_series(0, ?) g" rows])
               (jdbc/execute! tx ["INSERT INTO h_churn SELECT * FROM o_churn"])
               (jdbc/execute! tx
                              ["INSERT INTO o_toast
                                SELECT g, 0, CASE WHEN g % 2 = 0 THEN 'repeated' ELSE 'incompressible' END,
                                       repeat(md5((g * 7919)::text), 256),
                                       workload_checksum(g::text, '0',
                                         CASE WHEN g % 2 = 0 THEN 'repeated' ELSE 'incompressible' END,
                                         repeat(md5((g * 7919)::text), 256))
                                  FROM generate_series(0, ?) g" (quot rows 2)])
               (jdbc/execute! tx ["INSERT INTO h_toast SELECT * FROM o_toast"])
               (let [{:keys [logical_bytes main_buffers_bytes]}
                     (execute-one!
                      tx
                      ["SELECT
                          (SELECT coalesce(sum(octet_length(payload)), 0) FROM o_churn) +
                          (SELECT coalesce(sum(octet_length(payload)), 0) FROM o_toast) AS logical_bytes,
                          pg_size_bytes(current_setting('orioledb.main_buffers')) AS main_buffers_bytes"])]
                 (when (< logical_bytes (* multiplier main_buffers_bytes))
                   (throw (ex-info "preloaded working set is too small"
                                   {:logical-bytes logical_bytes
                                    :main-buffers-bytes main_buffers_bytes
                                    :multiplier multiplier})))
                 (jdbc/execute! tx
                                ["INSERT INTO workload_setup
                                  VALUES (1, ?, clock_timestamp(), ?, ?)
                                  ON CONFLICT (id) DO UPDATE SET
                                    version = EXCLUDED.version,
                                    initialized_at = EXCLUDED.initialized_at,
                                    logical_bytes = EXCLUDED.logical_bytes,
                                    main_buffers_bytes = EXCLUDED.main_buffers_bytes"
                                  schema-version logical_bytes main_buffers_bytes]))))))
        (jdbc/execute! connection ["CHECKPOINT"])
        (spit (io/file state-dir "setup.ready")
              (json/generate-string {:schema-version schema-version
                                     :ready-at (str (Instant/now))}))
        (finally
          (jdbc/execute! connection ["SELECT pg_advisory_unlock(730201)"]))))))

(defn read-json [name]
  (let [file (io/file state-dir name)]
    (when (.isFile file)
      (json/parse-string (slurp file) true))))

(defn line-count [file]
  (if (.isFile file)
    (with-open [reader (io/reader file)]
      (count (line-seq reader)))
    0))

(defn structural-verification! [connection]
  (doseq [table ["o_append" "o_churn" "o_toast"]]
    (let [rows (jdbc/execute! connection
                              [(str "SELECT * FROM verify_orioledb('" table "'::regclass, true)")]
                              builder)]
      (when (seq rows)
        (throw (ex-info "verify_orioledb reported corruption"
                        {:table table :rows (take 10 rows)})))))
  (bump! :structural-verifications)
  true)

(defn final-verification! []
  (let [test (environment-test)
        driver (read-json "driver-summary.json")]
    (with-open [connection (open-retrying test 300)]
      (let [current (generation connection)]
        (in-transaction!
         connection
         (fn [tx]
           (jdbc/execute! tx ["INSERT INTO recovery_write_probe(note) VALUES ('final rollback')"])
           (.rollback ^Connection tx)))
        (let [pre-checkpoint-data-verification (verify-data! connection)]
          (checkpoint! connection "final")
          (let [structural-verification (structural-verification! connection)
                data-verification (verify-data! connection)
                ddl-verification (verify-ddl-state! connection)]
            (jdbc/execute! connection
                           ["INSERT INTO verification_epochs(boot_id, final) VALUES (?, true)"
                            (:boot_id current)])
            (let [database-counts (execute-one!
                                   connection
                                   ["SELECT
                                       (SELECT count(*) FROM checkpoint_attempts WHERE completed_at IS NOT NULL) AS completed_checkpoints,
                                       (SELECT count(*) FROM recovery_probes) AS recovery_probes,
                                       (SELECT count(DISTINCT boot_id) FROM recovery_probes) AS ready_boots,
                                       (SELECT count(*) FROM ddl_history) AS ddl_operations,
                                       (SELECT count(DISTINCT boot_id) FROM ddl_history) AS ddl_boots,
                                       (SELECT count(*) FROM checkpoint_attempts WHERE kind = 'post-recovery' AND completed_at IS NOT NULL) AS post_recovery_checkpoints,
                                       (SELECT count(*) FROM checkpoint_attempts WHERE completed_boot_id = current_setting('cluster_name')) AS current_generation_checkpoints,
                                       (SELECT count(*) FROM checkpoint_attempts WHERE completed_at IS NULL) AS incomplete_checkpoints"])
                  state-canary (io/file state-dir "volume-canary")
                  canary-verified (io/file state-dir "canary-verified-after-restart")
                  summary {:valid (and driver (:valid driver)
                                       (zero? (:operation-failures driver 0)))
                           :driver driver
                           :database database-counts
                           :generation current
                           :boot-attempts (line-count (io/file state-dir "boot-attempts.jsonl"))
                           :volume-canary-present (.isFile state-canary)
                           :volume-canary-verified-after-restart (.isFile canary-verified)
                           :pre-checkpoint-data-verification pre-checkpoint-data-verification
                           :data-verification data-verification
                           :ddl-verification ddl-verification
                           :structural-verification structural-verification
                           :finished-at (str (Instant/now))}]
              (atomic-json! "final-summary.json" summary)
              summary)))))))

(defn -main [& [mode]]
  (case mode
    "initialize" (initialize!)
    "final" (final-verification!)
    (throw (ex-info "usage: checkpoint-recovery initialize|final" {:mode mode}))))
