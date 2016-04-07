(ns io.mandoline.backend.dynamodb.impl
  "A namespace for things we want to be private, but can't be tested privately
  (e.g. macros).")

(defmacro rethrow-aws-exception-with-info
  "Rethrow any AWS Exceptions with the query info attached for easier
  debugging."
  [info & body]
  `(let [;; Evaluate the info expression immediately to help prevent it from
         ;; producing errors that only happen when there's an Amazon exception.
         info# ~info]
     (try
       (do ~@body)
       (catch com.amazonaws.AmazonServiceException e#
         (throw (doto e#
                  (.setErrorMessage (format "%s (DynamoDB access %s)"
                                            (.getErrorMessage e#) info#))))))))
