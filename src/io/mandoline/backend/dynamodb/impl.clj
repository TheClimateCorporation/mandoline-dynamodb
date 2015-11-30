(ns io.mandoline.backend.dynamodb.impl
  "A namespace for things we want to be private, but can't be tested privately
  (e.g. macros).")

(defmacro rethrow-throughput-exception-with-table
  "Rethrow any ProvisionedThroughputExceededExceptions with the table attached
  for easier debugging."
  [table & body]
  `(let [;; Evaluate the table expression immediately to help prevent it from
         ;; producing errors that only happen when there's a throughput
         ;; exception.
         table# ~table]
     (try
       (do ~@body)
       (catch com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException e#
         (throw (doto e#
                  (.setErrorMessage (format "%s (table %s)"
                                            (.getErrorMessage e#) table#))))))))
