(ns lambdaisland.plenish-cloud.errors)

(defn rec-ex-data
  "Like ex-data, but merges in the data from all 'cause' exceptions.
   The inner most exceptions win key collisions."
  [err]
  (merge
   (ex-data err)
   (when-let [cause (.getCause err)]
     (rec-ex-data cause))))

(defmacro with-error-info
  "Macro to execute the given block, but wrap any exceptions with additional metadata.
   The error is still thrown."
  [info & body]
  `(try ~@body
        (catch Throwable e#
          (throw (ex-info (ex-message e#) ~info e#)))))
