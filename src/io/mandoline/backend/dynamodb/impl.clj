(ns io.mandoline.backend.dynamodb.impl
  "A namespace for things we want to be private, but can't be tested privately
  (e.g. macros).")

(defmacro rethrow-aws-exception-with-table
  "Rethrow any AWS Exceptions with the table attached for easier debugging."
  [table & body]
  `(let [;; Evaluate the table expression immediately to help prevent it from
         ;; producing errors that only happen when there's an Amazon exception.
         table# ~table]
     (try
       (do ~@body)
       (catch com.amazonaws.AmazonServiceException e#
         (throw (doto e#
                  (.setErrorMessage (format "%s (DynamoDB table %s)"
                                            (.getErrorMessage e#) table#))))))))
