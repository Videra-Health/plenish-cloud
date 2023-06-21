(ns lambdaisland.plenish.util)

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
   Some of the algorithms are simpler if that felse isn't present, and can thus assume that any retraction
   is a true deletion.
   Does not work well for cardinality-many attributes."
  [datoms]
  (let [have-positive-assertions (->> datoms
                                      (filter :added)
                                      (group-by index-datom-by-eid-attrid))]
    (remove (fn [datom]
              (and
               (-> datom :added not)
               ;;(-> datom :a cardinality-many-attr-ids not) We don't need to support cardinality many for now
               (-> datom index-datom-by-eid-attrid have-positive-assertions)))
            datoms)))