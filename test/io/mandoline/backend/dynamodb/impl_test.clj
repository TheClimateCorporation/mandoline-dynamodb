(ns io.mandoline.backend.dynamodb.impl-test
  (:require
    [clojure.test :refer :all]
    [io.mandoline.backend.dynamodb.impl :as ddbi]))

(deftest ^:unit test-rethrow-aws-exception-with-info
  (testing "rethrow-aws-exception-with-info is sane"
    (is (= :result
           (ddbi/rethrow-aws-exception-with-info {:table "table" :key :key}
             :ignored :result))))
  (testing "rethrow-aws-exception-with-info rethrows"
    (is (thrown-with-msg? com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
          #".*message.*(DynamoDB access \{:table \"mytable\", :key :key\}).*"
          (ddbi/rethrow-aws-exception-with-info {:table "mytable" :key :key}
            (throw (com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.
                     "message"))))))
  (testing "rethrow-aws-exception-with-info ignores other exceptions"
    (is (thrown-with-msg? NullPointerException #"^hi$"
          (ddbi/rethrow-aws-exception-with-info {:table "mytable" :key :key}
            (throw (NullPointerException. "hi")))))))

