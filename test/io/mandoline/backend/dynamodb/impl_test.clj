(ns io.mandoline.backend.dynamodb.impl-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline.backend.dynamodb.impl :as ddbi]))

(deftest ^:unit test-rethrow-aws-exception-with-table
  (testing "rethrow-aws-exception-with-table is sane"
    (is (= :result
           (ddbi/rethrow-aws-exception-with-table "table"
             :ignored :result))))
  (testing "rethrow-aws-exception-with-table rethrows"
    (is (thrown-with-msg? com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
          #".*message.*(DynamoDB table mytable).*"
          (ddbi/rethrow-aws-exception-with-table "mytable"
            (throw (com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.
                     "message"))))))
  (testing "rethrow-aws-exception-with-table ignores other exceptions"
    (is (thrown-with-msg? NullPointerException #"^hi$"
          (ddbi/rethrow-aws-exception-with-table "mytable"
            (throw (NullPointerException. "hi")))))))

