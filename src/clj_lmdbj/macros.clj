(ns clj-lmdbj.macros)

(defmacro with-tx [bindings & body]
  (if (and (even? (count bindings))
           (symbol? (bindings 0)))
    (let [tx-var (bindings 0)]
      `(let ~bindings
         (try
           (let [res# (do ~@body)]
             (if (= :read (:type ~tx-var))
               (.abort ~tx-var)
               (.commit ~tx-var))
             res#)
           (catch Exception e#
             (.abort ~tx-var)
             (throw e#)))))
    (throw (IllegalArgumentException.
            "with-tx requires an even number of bindings with the first one a symbol to hold the transaction.")))
  )
