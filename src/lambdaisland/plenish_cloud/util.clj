(ns lambdaisland.plenish-cloud.util 
  (:require [clojure.string :as str]))

(defn humanize-time-duration
  "Takes an elapsed time in milliseconds and prints something readable by humans."
  [ms]
  (let [seconds (/ (float ms) 1000)
        minutes (/ seconds 60)
        hours (/ minutes 60)
        days (/ hours 24)
        parts [[days "days"] [hours "hrs"] [minutes "mins"] [seconds "secs"] [ms "ms"]]
        part (first (filter #(-> % first (>= 1)) parts))]
    (if part
      (format "%.1f %2$s" (-> part first float) (second part))
      "N/A")))

(defn clamp [date min max]
  (cond
    (> date max) max
    (< date min) min
    :else date))

(def ^:private index-datom-by-eid-attrid (juxt :e :a))

(defn remove-update-retracts
  "The transaction log will record a false then true assertion for updates to an already-existing cardinality-one datoms.
   Some of the algorithms are simpler if that false isn't present, because they can assume that any retraction is a true deletion.
   Not for cardinality-many attributes."
  [datoms]
  (let [have-positive-assertions (->> datoms
                                      (filter :added)
                                      (group-by index-datom-by-eid-attrid))]
    (remove (fn [datom]
              (and
               (-> datom :added not)
               (-> datom index-datom-by-eid-attrid have-positive-assertions)))
            datoms)))

(defn dash->underscore
  "Replace dashes with underscores in string s"
  [s]
  (str/replace s #"-" "_"))

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