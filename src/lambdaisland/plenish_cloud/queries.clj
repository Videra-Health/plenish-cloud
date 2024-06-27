(ns lambdaisland.plenish-cloud.queries
  (:require [clojure.set :as set]
            [datomic.client.api :as d]
            [lambdaisland.plenish-cloud.util :as util]
            [next.jdbc :as jdbc]))

(defn tx->t-by-search
  "Finds the transaction t for the given tx entity id.
   This is fairly slow (about a minute) and I'm not certain how it will scale."
  [conn desired-tx-id]
  (prn "Having to search for tx...")
  (let [db (d/db conn)
        desired-tx-date (.getTime (:db/txInstant (d/pull db '[:db/txInstant] desired-tx-id)))]
    (loop [low-t 6 high-t (:t db)]
      (if (> low-t high-t)
        nil ;; tx not found
        (let [mid-t (quot (+ low-t high-t) 2)
              {:keys [t data]} (first (d/tx-range conn {:start mid-t :limit 1}))
              mid-tx-date-attr (->> data (filter #(-> % :a (= 50))) first)
              mid-tx-date (-> mid-tx-date-attr :v (.getTime))
              mid-tx-id (:e mid-tx-date-attr)]
          (cond (= mid-tx-id desired-tx-id) mid-t
                (< mid-tx-date desired-tx-date) (recur (inc mid-t) high-t)
                (> mid-tx-date desired-tx-date) (recur low-t (dec mid-t))))))))

(defn tx->t-by-query
  "Finds the transaction t for the given tx entity id. Uses the transaction table if that's available."
  [pg-conn desired-tx-id]
  (->>
   (jdbc/execute! pg-conn ["SELECT t FROM raw.transactions WHERE db__id = ?" desired-tx-id])
   first
   :transactions/t))

(defn find-latest-recorded-tx-of-attr
  "Given a membership attribute, estimates the latest transaction that might have recorded it into SQL.
   It's an estimation because we don't keep track of when each row was created, so we estimate the latest by the greatest id."
  [ctx datomic-conn pg-conn missed-attr]
  (let [table-of-missing-attr (util/table-name ctx missed-attr)
        ids (try (map (comp second first) ;; Unwraps the query result w/o having to know the table name
                      (jdbc/execute! pg-conn [(str "SELECT db__id FROM raw ." table-of-missing-attr)]))
                 (catch org.postgresql.util.PSQLException _e
                   ;; Likely the table doesn't exist, so there are no latest transactions on it. We'd expect to return nil.
                   nil))
        tx (when ids (->> (d/q '[:find ?tx ?when
                                 :in $ [?e ...]
                                 :where
                                 [?e _ _ ?tx]
                                 [?tx :db/txInstant ?when]]
                               (d/db datomic-conn) ids)
                          (sort-by second)
                          ffirst))]
    tx))

(defn first-tx-of-attr
  "Given an attribute, finds the first transaction that involved it."
  [datomic-conn attr]
  (ffirst (d/q '[:find (min ?tx)
                 :in $ ?attr
                 :where
                 [?e ?attr ?val ?tx]]
               (d/db datomic-conn) attr)))

(defn retrying-tx-range [datomic-conn start end]
  (let [full-range (d/tx-range datomic-conn {:start start
                                             :end end
                                             :limit -1})]
    ((fn repeat [resume-at range]
       (lazy-seq
        (try
          (let [[x & xs] range]
            (when x ;; Stopping point
              (cons x (repeat (-> x :t inc) xs))))
          (catch Exception e
            (if (-> e ex-data :cognitect.anomalies/message (= "Specified iterator was dropped"))
              (do
                (prn "Lost connection at: " resume-at ", but retrying..." [])
                (retrying-tx-range datomic-conn resume-at end))
              (do
                (prn "Other error"
                     (-> e ex-data :cognitect.anomalies/message (= "Specified iterator was dropped"))
                     (-> e ex-data :cognitect.anomalies/message)
                     (-> e ex-data))
                (throw e)))))))
     start full-range)))

(defn remove-partition-datoms
  "Removes datoms recording to the partition. They confuse plenish and we don't have a table for them anyway."
  [txes]
  (map (fn [tx] (update tx :data (fn [data]
                                   (remove #(-> % :e (= 0)) data))))
       txes))

(defn ids-of-attrs-sharing-namespace [datomic-conn attr]
  (->> (d/q '[:find ?e
              :in $ ?namespace
              :where
              [?e :db/ident ?ident]
              [(namespace ?ident) ?namespace]]
            (d/db datomic-conn)
            (namespace attr))
       (map first)
       set))

(defn filter-datoms-by-attrs
  [attrs txes]
  (->> txes
       (filter (fn [tx]
                 (->> tx
                      :data
                      (map :a)
                      set
                      (set/intersection attrs)
                      seq)))))