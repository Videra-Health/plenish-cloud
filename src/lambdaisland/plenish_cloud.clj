(ns lambdaisland.plenish-cloud
  "Transfer datomic data into a relational target database, transaction per
  transaction."
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [datomic.client.api :as d]
            [honey.sql :as honey]
            [lambdaisland.plenish-cloud.util :as util]
            [next.jdbc :as jdbc])
  (:import [java.util Date]))

(set! *warn-on-reflection* true)

(def ^:dynamic *debug* (= (System/getenv "PLENISH_DEBUG") "true"))
(defn dbg [& args] (when *debug* (prn args)))

;;;;;;;;;;;;;;;;
;; Datom helpers

(def -e "Get the entity id of a datom" :e)
(def -a "Get the attribute of a datom" :a)
(def -v "Get the value of a datom" :v)
(def -t "Get the transaction number of a datom" :tx)
(def -added? "Has this datom been added or retracted?" :added)

;;;;;;;;;;;;;;;;;;;
;; Context helpers

;; All through the process we pass around a `ctx` map, which encapsulates the
;; state of the export/import process.

;; - `:idents` map from eid (entity id) to entity/value map
;; - `:entids` map from ident to eid (reverse lookup)
;; - `:tables` map from membership attribute to table config, as per Datomic
;;   Analytics metaschema We also additionally store a `:columns` map for each
;;   table, to track which columns already exist in the target db.
;; - `:db-types` mapping from datomic type to target DB type
;; - `:ops` vector of "operations" that need to be propagated, `[:upsert ...]`, `[:delete ...]`, etc.

(defn ctx-ident
  "Find an ident (keyword) by eid"
  [ctx eid]
  (get-in ctx [:idents eid :db/ident]))

(defn ctx-entid
  "Find the numeric eid for a given ident (keyword)"
  [ctx ident]
  (get-in ctx [:entids ident]))

(defn ctx-valueType
  "Find the valueType (keyword, e.g. `:db.type/string`) for a given attribute (as eid)"
  [ctx attr-id]
  (ctx-ident ctx (get-in ctx [:idents attr-id :db/valueType])))

(defn ctx-cardinality
  "Find the cardinality (`:db.cardinality/one` or `:db.cardinality/many`) for a
  given attribute (as eid)"
  [ctx attr-id]
  (ctx-ident ctx (get-in ctx [:idents attr-id :db/cardinality])))

(defn ctx-card-many?
  "Returns true if the given attribute (given as eid) has a cardinality of `:db.cardinality/many`"
  [ctx attr-id]
  (= :db.cardinality/many (ctx-cardinality ctx attr-id)))

(defn dash->underscore
  "Replace dashes with underscores in string s"
  [s]
  (str/replace s #"-" "_"))

(defn has-attr?
  "Does the entity `eid` have the attribute `attr` in the database `db`.

  Uses direct index access so we can cheaply check if a give entity should be
  added to a given table, based on a membership attribute."
  [db eid attr]
  (-> db
      ^Iterable (d/datoms {:index :eavt :components [eid attr]})
      .iterator
      .hasNext))

;; A note on naming, PostgreSQL (our primary target) does not have the same
;; limitations on names that Presto has. We can use e.g. dashes just fine,
;; assuming names are properly quoted. We've still opted to use underscores by
;; default, to make ad-hoc querying easier (quoting will often not be
;; necessary), and to have largely the same table structure as datomic
;; analytics, to make querying easier.
;;
;; That said we don't munge anything else (like a trailing `?` and `!`), and do
;; rely on PostgreSQL quoting to handle these.

(defn table-name
  "Find a table name for a given table based on its membership attribute, either
  as configured in the metaschema (`:name`), or derived from the namespace of
  the membership attribute."
  [ctx mem-attr]
  (get-in ctx
          [:tables mem-attr :name]
          (dash->underscore (namespace mem-attr))))

(defn join-table-name
  "Find a table name for a join table, i.e. a table that is created because of a
  cardinality/many attribute, given the membership attribute of the base table,
  and the cardinality/many attribute. Either returns a name as configured in the
  metaschema under `:rename-many-table`, or derives a name as
  `namespace_mem_attr_x_name_val_attr`."
  [ctx mem-attr val-attr]
  (get-in ctx
          [:tables mem-attr :rename-many-table val-attr]
          (dash->underscore (str (table-name ctx mem-attr) "_x_" (name val-attr)))))

(defn column-name
  "Find a name for a column based on the table membership attribute, and the
  attribute whose values are to be stored in the column."
  [ctx mem-attr col-attr]
  ;;{:pre [(some? col-attr)]}
  (get-in ctx
          [:tables mem-attr :rename col-attr]
          (dash->underscore (name col-attr))))

(defn attr-db-type
  "Get the target db type for the given attribute."
  [ctx attr-id]
  (get-in ctx [:db-types (ctx-valueType ctx attr-id)]))

;; Transaction processing logic

(defn track-idents
  "Keep `:entids` and `:idents` up to date based on tx-data. This allows us to
  incrementally track schema changes, so we always have the right metadata at
  hand."
  ;; This has a notable shortcoming in that we currently don't treat
  ;; `cardinality/many` in any special way. In the initial `pull-idents` these will
  ;; come through as collections, but any later additions will replace these with
  ;; single values.
  ;;
  ;; However, the attributes we care about so far (`:db/ident`,
  ;; `:db/cardinality`, `:db/valueType`) are all of cardinality/one, so for the
  ;; moment this is not a great issue.
  [ctx tx-data]
  (let [db-ident  (get-in ctx [:entids :db/ident])
        tx-idents (util/remove-update-retracts (filter #(= db-ident (-a %)) tx-data))
        tx-rest   (remove #(= db-ident (-a %)) tx-data)]
    (as-> ctx ctx
      ;; Handle `[_ :db/ident _]` datoms first. We can't rely on any ordering of
      ;; datoms within a transaction. If a transaction adds both `:db/ident` and
      ;; other attributes, then we need to process the `:db/ident` datom first,
      ;; so that when we process the rest of the datoms we can see that this eid
      ;; is an ident.
      (reduce (fn [ctx datom]
                (let [e     (-e datom)
                      ident (-v datom)]
                  (if (-added? datom)
                    (-> ctx
                        (update :entids assoc ident e)
                        (update-in [:idents e] assoc
                                   :db/id e
                                   :db/ident ident))
                    (-> ctx
                        (update :entids dissoc ident)
                        (update :idents dissoc e)))))
              ctx
              tx-idents)
      ;; Handle non `[_ :db/ident _]` datoms
      ;; This fills in all of the facts about the attribute (e.g. :db/cardinality)
      ;; after the above created it in (:idents ctx)
      ;; Testing shows that the old column is deleted, the new column is created, and data in the old column is lost
      ;; Not likely what we want, but could work for existing use cases
      (->> tx-rest
           (filter #(get-in ctx [:idents (-e %)]))
           util/remove-update-retracts ;; We run this a bit later rather than at the top for speed
           (reduce (fn [ctx datom]
                     (-> ctx
                         (update-in
                          [:idents (-e datom)]
                          (fn [m]
                            (let [attr (ctx-ident ctx (-a datom))]
                              (if (-added? datom)
                                (assoc m attr (-v datom))
                                (dissoc m attr)))))))
                   ctx)))))

(def pg-min-date -210898425600000)

(def pg-max-date 9224286307200000)

(defn encode-value
  "Do some pre-processing on a value based on the datomic value type, so that
  HoneySQL/JDBC is happy with it. Somewhat naive and postgres specific right
  now."
  [ctx attr-id value]
  (let [datomic-type (ctx-valueType ctx attr-id)]
    (case datomic-type
      :db.type/ref (if (keyword? value)
                     (ctx-entid ctx value)
                     value)
      :db.type/tuple [:raw (str \' (str/replace (str (charred/write-json-str value)) "'" "''") \' "::jsonb")]
      :db.type/keyword (str (when (qualified-ident? value)
                              (str (namespace value) "/"))
                            (name value))
      :db.type/instant [:raw (format "to_timestamp(%.3f)"
                                     (double (/ (util/clamp (.getTime ^java.util.Date value) pg-min-date pg-max-date) 1000)))]
      value)))

;; The functions below are the heart of the process

;; process-tx                       -  process all datoms in a single transaction
;; \___ process-entity              -  process datoms within a transaction with the same entity id
;;      \____ card-one-entity-ops   -  process datoms for all cardinality/one attributes of a single entity
;;      \____ card-many-entity-ops  -  process datoms for all cardinality/many attributes of a single entity

(defn card-one-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/one` datoms in a
  single transaction and a single entity/table, identified by its memory
  attribute."
  [{:keys [tables] :as ctx} mem-attr eid datoms t]
  {:pre [(every? #(= eid (-e %)) datoms)]}
  (let [;; An update of an attribute will manifest as two datoms in the same
        ;; transaction, one with added=true, and one with added=false. In this
        ;; case we can ignore the added=false datom.
        datoms (util/remove-update-retracts datoms)
        ;; Figure out which columns don't exist yet in the target database. This
        ;; may find columns that actually do already exist, depending on the
        ;; state of the context. This is fine, we'll process these schema
        ;; changes in an idempotent way, we just want to prevent us from having
        ;; to attempt schema changes for every single transaction.
        missing-cols (sequence
                      (comp
                       (remove (fn [d]
                                 ;; If there's already a `:columns` entry in the
                                 ;; context, then this column already exists in
                                 ;; the target DB, if not it needs to be
                                 ;; created. This is heuristic, we do a
                                 ;; conditional alter table, so no biggie if it
                                 ;; already exists.
                                 (get-in ctx [:tables mem-attr :columns (ctx-ident ctx (-a d))])))
                       (map -a)
                       (map (fn [attr-id]
                              (let [attr (ctx-ident ctx attr-id)]
                                [attr
                                 {:name (column-name ctx mem-attr attr)
                                  :type (attr-db-type ctx attr-id)}]))))
                      datoms)
        ;; Do we need to delete the row corresponding with this entity.
        retracted? (and (some (fn [d]
                                ;; Datom with membership attribute was retracted,
                                ;; remove from table
                                (and (not (-added? d))
                                     (= mem-attr (ctx-ident ctx (-a d)))))
                              datoms)
                        (not (some (fn [d]
                                     ;; Unless the same transaction
                                     ;; immediately adds a new datom with the
                                     ;; membership attribute
                                     (and (-added? d)
                                          (= mem-attr (ctx-ident ctx (-a d)))))
                                   datoms)))
        table (table-name ctx mem-attr)
        ;; Note, it is possible to have a change to a prior transaction that itself is not a transaction!
        new-transaction? (and (= "transactions" table)
                              (some (fn [d]
                                      (and (= :db/txInstant (-a d))
                                           (-added? d)))
                                    datoms))]
    ;;(clojure.pprint/pprint ['card-one-entity-ops mem-attr eid datoms retracted?])
    (cond-> ctx
      ;; Evolve the schema
      (seq missing-cols)
      (-> (update :ops
                  (fnil conj [])
                  [:ensure-columns
                   {:table   table
                    :columns (into {} missing-cols)}])
          (update-in [:tables mem-attr :columns] (fnil into {}) missing-cols))
      ;; Delete/insert values
      :->
      (update :ops (fnil conj [])
              (if retracted?
                [:delete
                 {:table  table
                  :values {"db__id" eid}}]
                [(if new-transaction?
                   :insert
                   :upsert)
                 {:table table
                  :by #{"db__id"}
                  :values (into (cond-> {"db__id" eid}
                                    ;; Bit of manual fudgery to also get the "t"
                                    ;; value of each transaction into
                                    ;; our "transactions" table.
                                  new-transaction? (assoc "t" t))
                                (map (juxt #(column-name ctx mem-attr (ctx-ident ctx (-a %)))
                                           #(when (-added? %)
                                              (encode-value ctx (-a %) (-v %)))))
                                datoms)}])))))

(defn card-many-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/many` datoms in a
  single transaction and a single table, identified by its memory attribute.
  Each `:db.cardinality/many` attribute results in a separate two-column join
  table."
  [{:keys [tables] :as ctx} mem-attr eid datoms]
  (let [missing-joins (sequence
                       (comp
                        (remove #(get-in ctx [:tables mem-attr :join-tables (ctx-ident ctx (-a %))]))
                        (map -a)
                        (distinct)
                        (map (fn [attr-id]
                               (let [attr (ctx-ident ctx attr-id)]
                                 [attr
                                  {:name (column-name ctx mem-attr attr)
                                   :type (attr-db-type ctx attr-id)}]))))
                       datoms)]
    (cond-> ctx
      (seq missing-joins)
      (-> (update :ops
                  (fnil into [])
                  (for [[val-attr join-opts] missing-joins]
                    [:ensure-join
                     {:table (join-table-name ctx mem-attr val-attr)
                      :fk-table (table-name ctx mem-attr)
                      :val-attr val-attr
                      :val-col (column-name ctx mem-attr val-attr)
                      :val-type (:type join-opts)}]))
          (update-in [:tables mem-attr :join-tables] (fnil into {}) missing-joins))
      :->
      (update :ops
              (fnil into [])
              (for [d datoms]
                (let [attr-id (-a d)
                      attr (ctx-ident ctx attr-id)
                      value (-v d)
                      sql-table (join-table-name ctx mem-attr attr)
                      sql-col (column-name ctx mem-attr attr)
                      sql-val (encode-value ctx attr-id value)]
                  (if (-added? d)
                    [:upsert
                     {:table sql-table
                      :by #{"db__id" sql-col}
                      :values {"db__id" eid
                               sql-col sql-val}}]
                    [:delete
                     {:table sql-table
                      :values {"db__id" eid
                               sql-col sql-val}}])))))))

(def ignore-idents #{:db/ensure
                     :db/fn
                     :db.install/valueType
                     :db.install/attribute
                     :db.install/function
                     :db.entity/attrs
                     :db.entity/preds
                     :db.attr/preds})

(defn attrs-on-entity [db eid]
  (->> (d/datoms db {:index :eavt :components [eid] :limit -1})
       (map :a)
       set))

;; Some simplifying assumptions we're making:
;; 1. The membership attributes are never cardinality many attributes.
;;    The peer implemetnation was able to consult the DB to determine if an attribute was new cheaply, but cloud cannot do that.
;;    Instead we need to look at the transaction and assume that if there is no retraction in the same tx as an assertion for the
;;    membership attribute, that means the entity is new. However, cardinality-many attributes can have that even after the first attribute.
;;
;; 2. Entities must carry the membership attribute on their first assertion.
;;    That is, facts asserted in a TX before the membership attribute was given will not be synced to SQL.
;;    This is because it is very expensive to go back and find all of those old datoms anytime a new entity shows up.

(defn process-entity
  "Process the datoms within a transaction for a single entity. This checks all
  tables to see if the entity contains the membership attribute, if so
  operations get added under `:ops` to evolve the schema and insert the data."
  [{:keys [tables] :as ctx} existing-attributes eid datoms t]
  (reduce
   (fn [ctx [mem-attr _table-opts]]
     (if (or
          ;; Pre-existing entities
          (get existing-attributes mem-attr)
          ;; Or you're just now being created
          (some #(= mem-attr (ctx-ident ctx (-a %))) datoms))
       ;; Handle cardinality/one separate from cardinality/many
       (let [datoms           (remove (fn [d] (contains? ignore-idents (ctx-ident ctx (-a d)))) datoms)
             card-one-datoms  (remove (fn [d] (ctx-card-many? ctx (-a d))) datoms)
             card-many-datoms (filter (fn [d] (ctx-card-many? ctx (-a d))) datoms)]
         (cond-> ctx
           (seq card-one-datoms)
           (card-one-entity-ops mem-attr eid card-one-datoms t)

           (seq card-many-datoms)
           (card-many-entity-ops mem-attr eid card-many-datoms)))
       ctx))
   ctx
   tables))

(defn process-tx
  "Handle a single datomic transaction, this will update the context, appending to
  the `:ops` the operations needed to propagate the update, while also keeping
  the rest of the context (`ctx`) up to date, in particular by tracking any
  schema changes, or other changes involving `:db/ident`."
  [ctx conn {:keys [t data] :as tx}]
  ;;(prn "process-tx" t data)
  (try
    (let [ctx (track-idents ctx data)
          entities (group-by -e data)
          prev-db (d/as-of (d/db conn) (dec t))
          existing-attributes (->> (d/q '[:find ?e ?a
                                          :in $ [?e ...]
                                          :where [?e ?a]]
                                        prev-db (keys entities))
                                   (group-by first)
                                   (map (fn [[e attrs]]
                                          [e (set (map (comp (partial ctx-ident ctx) second) attrs))]))
                                   (into {}))]
      (reduce (fn [ctx [eid datoms]]
                (process-entity ctx (get existing-attributes eid) eid datoms t))
              ctx
              entities))
    (catch Exception e
      (prn "tx" tx)
      (spit "error-ctx.edn" (pr-str ctx))
      ;;(prn "ctx" ctx)
      (prn e))))

;; Up to here we've only dealt with extracting information from datomic
;; transactions, and turning them into
;; abstract "ops" (:ensure-columns, :upsert, :delete, etc). So this is all
;; target-database agnostic, what's left is to turn this into SQL and sending it
;; to the target.

;; Converting ops to SQL

(def pg-type
  {:db.type/ref :bigint
   :db.type/keyword :text
   :db.type/long :bigint
   :db.type/string :text
   :db.type/boolean :boolean
   :db.type/uuid :uuid
   :db.type/instant :timestamp ;; no time zone information in java.util.Date
   :db.type/double [:float 53]
   ;;   :db.type/fn
   :db.type/float [:float 24]
   :db.type/bytes :bytea
   :db.type/uri :text
   :db.type/bigint :numeric
   :db.type/bigdec :numeric
   :db.type/tuple :jsonb})

(defmulti op->sql
  "Convert a single operation (two element vector), into a sequence of HoneySQL
  maps."
  first)

(defmethod op->sql :ensure-columns [[_ {:keys [table columns]}]]
  (into
   [{:create-table [table :if-not-exists]
     :with-columns [[:db__id [:raw "bigint"] [:primary-key]]]}]
   (map (fn [[_ {:keys [name type]}]]
          {:alter-table [table]
           :add-column [(keyword name)
                        (if (keyword? type)
                          ;; Annoyingly this is needed because we use `:quote true`, and honey tries to quote the type
                          [:raw (clojure.core/name type)]
                          type)
                        :if-not-exists]}))
   columns))

(defmethod op->sql :insert [[_ {:keys [table by values]}]]
  [{:insert-into [(keyword table)]
    :values      [values]}])

(defmethod op->sql :upsert [[_ {:keys [table by values]}]]
  (let [op {:insert-into   [(keyword table)]
            :values        [values]
            :on-conflict   (map keyword by)}
        attrs (apply dissoc values by)]
    [(if (seq attrs)
       (assoc op :do-update-set (keys attrs))
       (assoc op :do-nothing []))]))

(defmethod op->sql :delete [[_ {:keys [table values]}]]
  [{:delete-from (keyword table)
    :where (reduce-kv (fn [clause k v]
                        (conj clause [:= k v]))
                      [:and]
                      (update-keys values keyword))}])

(defmethod op->sql :ensure-join [[_ {:keys [table val-col val-type]}]]
  [{:create-table [table :if-not-exists]
    :with-columns [[:db__id [:raw "bigint"]]
                   [(keyword val-col) (if (keyword? val-type)
                                        [:raw (name val-type)]
                                        val-type)]]}
   ;; cardinality/many attributes are not multi-set, a given triplet can only be
   ;; asserted once, so a given [eid value] for a given attribute has to be
   ;; unique.
   {::create-index {:on table
                    :name (str "unique_attr_" table "_" val-col)
                    :unique? true
                    :if-not-exists? true
                    :columns [:db__id (keyword val-col)]}}])

;; HoneySQL does not support CREATE INDEX. It does support adding indexes
;; through ALTER TABLE, but that doesn't seem to give us a convenient way to
;; sneak in the IF NOT EXISTS. Namespacing this becuase it's a pretty
;; bespoke/specific implementation which we don't want to leak into application
;; code.
(honey/register-clause!
 ::create-index
 (fn [clause {:keys [on name unique? if-not-exists? columns]}]
   [(str "CREATE " (when unique? "UNIQUE ") "INDEX "
         (when if-not-exists? "IF NOT EXISTS ")
         (when name (str (honey/format-entity name) " "))
         "ON " (honey/format-entity on)
         " (" (str/join ", " (map honey/format-entity columns)) ")")])
 :alter-table)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Top level process

(defn- pull-idents
  "Do a full `(pull [*])` of all database entities that have a `:db/ident`, used
  to bootstrap the context, we track all idents and their
  metadata (`:db/valueType`, `:db/cardinality` etc in memory on our inside
  inside a `ctx` map)."
  [db]
  (map
   (fn [ident-attrs]
     (update-vals
      ident-attrs
      (fn [v]
        (if-let [id (when (map? v)
                      (:db/id v))]
          id
          v))))
   (map first
        (d/q
    ;;'[:find [(pull ?e [*]) ...] ;; https://docs.datomic.com/pro/query/query.html#find-specifications - can be simulated with a (map first)
         '[:find (pull ?e [*])
           :where [?e :db/ident]]
         db))))

(defn find-first-tx
  "Cloud databases start at t=6. All prior attributes are for system datoms, and you'd be very unhappy trying to sync those."
  [conn]
  (->>
   (d/tx-range conn {:start 5 ;; It's never before 10
                     :limit 10}) ;; It's always within the first 15 datoms
   (remove (fn [{:keys [data]}] (some #(= 0 (:e %)) data)))
   first
   :t))

(defn initial-ctx
  "Create the context map that gets passed around all through the process,
  contains both caches for quick lookup of datomic schema information,
  configuration regarding tables and target db, and eventually `:ops` that need
  to be processed."
  ([conn metaschema]
   (initial-ctx conn metaschema nil))
  ([conn metaschema max-t]
   ;; Bootstrap, make sure we have info about idents that datomic creates itself
   ;; at db creation time. d/as-of t=999 is basically an empty database with only
   ;; metaschema attributes (:db/txInstant etc), since the first "real"
   ;; transaction is given t=1000. Interesting to note that Datomic seems to
   ;; bootstrap in pieces: t=0 most basic idents, t=57 add double, t=63 add
   ;; docstrings, ...
   (let [start-t (if max-t (inc max-t) (find-first-tx conn))
         end-t (:t (d/db conn))
         idents (pull-idents (d/as-of (d/db conn) start-t))]
     {;; Track datomic schema
      :start-t start-t
      :end-t end-t ;; Last transaction we'll process (inclusive)
      :start-time (.getTime (Date.))
      :entids (into {} (map (juxt :db/ident :db/id)) idents)
      :idents (into {} (map (juxt :db/id identity)) idents)
      ;; Configure/track relational schema
      :tables (-> metaschema
                  :tables
                  (update :db/txInstant assoc :name "transactions")
                  (update :db/ident assoc :name "idents"))
      ;; Mapping from datomic to relational type
      :db-types pg-type
      ;; Create two columns that don't have a attribute as such in datomic, but
      ;; which we still want to track
      :ops [[:ensure-columns
             {:table   "idents"
              :columns {:db/id {:name "db__id"
                                :type :bigint}}}]
            [:ensure-columns
             {:table   "transactions"
              :columns {:t {:name "t"
                            :type :bigint}}}]]})))


(def report-frequency (* 1000 60 60 24)) ;; 1 day

(defn maybe-report [{:keys [last-report start-time end-t start-t] :as ctx} tx]
  (let [current-t (:t tx)
        datoms (:data tx)
        tx-date (-v (first (filter #(=  :db/txInstant (ctx-ident ctx (-a %))) datoms)))]
    (if (and (not= current-t start-t)
             (or (nil? last-report)
                 ;; It's been awhile (30 seconds)
                 (-> (Date.) (.getTime) (- (:time last-report)) (> 30000))
                 ;; We're on a new day
                 (> (- (.getTime ^java.util.Date tx-date) (.getTime ^java.util.Date (:tx-time last-report)))
                    report-frequency)))
      (let [progress-ratio (/ (float current-t) end-t)
            progress-percent (* progress-ratio 100)
            now (.getTime (Date.))
            elapsed (- now start-time)
            resume-adjusted-progress-ratio (/ (float (- current-t start-t)) (- end-t start-t)) ;; If we're resuming, then a ton of work was already done, and throws off estimates
            estimated-total-elapsed (/ (float elapsed) resume-adjusted-progress-ratio)
            remaining (- estimated-total-elapsed elapsed)]
        (println (format "-> %1$tY-%1$tm-%1$td - progress %2$,d / %3$,d = %4$.1f%% Elapsed: %5$s Remaining: %6$s"
                         tx-date current-t  end-t progress-percent
                         (util/humanize-time-duration elapsed)
                         (util/humanize-time-duration remaining)))
        (assoc ctx :last-report {:tx-time tx-date :time (.getTime (Date.))}))
      ctx)))

(defn import-tx-range
  "Import a range of transactions (e.g. from [[d/tx-range]]) into the target
  database. Takes a `ctx` as per [[initial-ctx]], a datomic connection `conn`,
  and a JDBC datasource `ds`"
  [ctx conn ds tx-range]
  (loop [ctx ctx
         [tx & txs] tx-range
         cnt 1]
    (if tx
      (let [ctx (maybe-report ctx tx)
            ctx (process-tx ctx conn tx)
            queries (eduction
                     (comp
                      (mapcat op->sql)
                      (map #(honey/format % {:quoted true})))
                     (:ops ctx))]
        ;; Each datomic transaction gets committed within a separate JDBC
        ;; transaction, and this includes adding an entry to the "transactions"
        ;; table. This allows us to see exactly which transactions have been
        ;; imported, and to resume work from there.
        (dbg 't '--> (:t tx))
        (try
          (jdbc/with-transaction [jdbc-tx ds]
            (run! dbg (:ops ctx))
            (run! #(do (dbg %)
                       (jdbc/execute! jdbc-tx %)) queries))
          (catch Exception e
            (spit "error-ctx.edn" (pr-str ctx))
            (throw (ex-info "Failed to run sql" {:tx tx
                                                 :ops (:ops ctx)
                                                 :queries queries}
                            e))))

        (recur (dissoc ctx :ops) txs
               (inc cnt)))
      ctx)))

(defn find-max-t
  "Find the highest value in the transactions table in postgresql. The sync should
  continue from `(inc (find-max-t ds))`"
  [ds]
  (:max
   (first
    (try
      (jdbc/execute! ds ["SELECT max(t) FROM transactions"])
      (catch Exception e
        ;; If the transactions table doesn't yet exist, return `nil`, so we start
        ;; from the beginning of the log
        nil)))))

(defn sync-to-latest
  "Convenience function that combines the ingredients above for the common case of
  processing all new transactions up to the latest one."
  [datomic-conn pg-conn metaschema]
  (let [;; Find the most recent transaction that has been copied, or `nil` if this
        ;; is the first run
        max-t (find-max-t pg-conn)
        ;; Query the current datomic schema. Plenish will track schema changes as
        ;; it processes transcations, but it needs to know what the schema looks
        ;; like so far.
        ctx   (initial-ctx datomic-conn metaschema max-t)
        _ (spit "start-ctx.edn" (pr-str ctx))

        ;; Grab the datomic transactions you want Plenish to process. This grabs
        ;; all transactions that haven't been processed yet.
        ;;txs   (d/tx-range (d/log datomic-conn) (when max-t (inc max-t)) nil)
        txs (d/tx-range datomic-conn {:start (:start-t ctx)
                                      :end (inc (:end-t ctx)) ;; :end is exclusive
                                      :limit -1})]

    ;; Get to work
    (import-tx-range ctx datomic-conn pg-conn txs)))

