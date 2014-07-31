(ns io.mandoline.backend.dynamodb-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [clojure.string :refer [split join]]
    [taoensso.faraday :as far]
    [io.mandoline :as db]
    [io.mandoline.impl :as impl]
    [io.mandoline.slice :as slice]
    [io.mandoline.backend.dynamodb :as ddb]
    [io.mandoline.test.utils :refer [random-name
                                             with-and-without-caches
                                             with-temp-db]]
    [io.mandoline.test
     [concurrency :refer [lots-of-overlaps
                          lots-of-processes
                          lots-of-tiny-slices]]
     [entire-flow :refer [entire-flow]]
     [failed-ingest :refer [failed-write]]
     [grow :refer [grow-dataset]]
     [linear-versions :refer [linear-versions]]
     [nan :refer [fill-double
                  fill-float
                  fill-short]]
     [overwrite :refer [overwrite-dataset
                        overwrite-extend-dataset]]
     [scalar :refer [write-scalar]]
     [shrink :refer [shrink-dataset]]]
    [io.mandoline.test.protocol
     [chunk-store :as chunk-store]
     [schema :as schema]])
  (:import
    [java.nio ByteBuffer]
    [org.apache.commons.codec.digest DigestUtils]
    [com.amazonaws AmazonClientException]
    [com.amazonaws.auth
     AWSCredentialsProvider
     BasicAWSCredentials
     DefaultAWSCredentialsProviderChain]
    [com.amazonaws.retry PredefinedRetryPolicies]
    [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient]
    [com.amazonaws.services.dynamodbv2.model
     AttributeValue
     GetItemRequest
     GetItemResult
     LimitExceededException
     ResourceInUseException
     ResourceNotFoundException
     ProvisionedThroughputExceededException]
    [io.mandoline.slab Slab]))

(def ^:private test-root-base
  "integration-testing.mandoline.io")

(def test-ddb-throughputs
  {:chunks {:read 100 :write 100}
   :indices {:read 50 :write 50}
   :versions {:read 4 :write 2}})

; Run each test with binding to reduced DynamoDB throughouts.
(use-fixtures :each
  (fn [f]
    (binding [ddb/*default-throughputs* test-ddb-throughputs]
      (f))))

(defn- table-exists?
  [creds table]
  (far/describe-table creds table))

(def ^:private test-dataset-name "test-dataset")

(defn setup
  "Create a random store spec for testing the DynamoDB Mandoline
  backend.

  This function is intended to be used with the matching teardown
  function."
  []
  (let [root (format "%s.%s" (random-name) test-root-base)
        dataset test-dataset-name]
    {:store "io.mandoline.backend.dynamodb/mk-schema"
     :root root
     :dataset dataset}))

(defn localize
  "This returns client-opts for faraday to use to connect to the local
  dynamodb.

  These client-opts contain empty credentials and a local :endpoint"
  [store-spec]
  {:endpoint "http://localhost:8000"
   :secret-key ""
   :access-key ""})

(defn teardown
  [store-spec]
  (let [s (ddb/mk-schema store-spec)]
    (with-test-out
      (println
        "Please wait for post-test cleanup of DynamoDB tables."
        "This may take a while.")
      (try
        (.destroy-dataset s test-dataset-name)
        ; Swallow any exception after making a best effort
        (catch Exception e
          (println
            (format "Failed to destroy the dataset: %s %s" test-dataset-name e)))))))

(defmacro deftest* [test-name & body]
  "A macro to generate two deftests.

   One to run against integration dynamodb, marked with ^:integration,
   and the other to run against a local dynamodb, marked with ^:local"
  (let [old-meta (meta test-name)]
  `(do
     (deftest ~(vary-meta (symbol (str test-name "-local")) merge old-meta {:local true})
       (try
         (with-redefs [io.mandoline.backend.dynamodb/store-spec->client-opts localize]
           ~@body)
         (catch AmazonClientException e#
           (if (re-find #"Connection to http://localhost:8000 refused" (.getMessage e#))
             (do
              (binding [*out* *err*]
                (println
                 "Unable to connect the local instance of DynamoDB on http://localhost:8000\n"
                 "See the README.md for instructions on running these tests\n"))
               (is false))
             (throw e#)))))
     (deftest ~(vary-meta (symbol (str test-name "-integration")) merge old-meta {:integration true})
       ~@body))))

; This is part of the following test
(deftest* ^:some-metadata foo nil)

(deftest test-deftest*-metadata
  "A test to make sure the metadata is preserved when using deftest*"
  (is foo-local)
  (is foo-integration)
  (is (contains? (meta (var foo-local)) :some-metadata))
  (is (contains? (meta (var foo-integration)) :some-metadata)))

(deftest* test-dynamodb-chunk-store-properties
  (let [store-specs (atom {}) ; map of ChunkStore instance -> store-spec
        setup-chunk-store (fn []
                            (let [store-spec (setup)
                                  dataset (:dataset store-spec)
                                  c (-> (doto (ddb/mk-schema store-spec)
                                          (.create-dataset dataset))
                                      (.connect dataset)
                                      (.chunk-store {}))]
                              (swap! store-specs assoc c store-spec)
                              c))
        teardown-chunk-store (fn [c]
                               (let [store-spec (@store-specs c)]
                                 (teardown store-spec)))
        num-chunks 10]
    (chunk-store/test-chunk-store-properties-single-threaded
      setup-chunk-store teardown-chunk-store num-chunks)
    (chunk-store/test-chunk-store-properties-multi-threaded
      setup-chunk-store teardown-chunk-store num-chunks)))

(deftest* test-dynamodb-schema-properties
  (let [store-specs (atom {}) ; map of Schema instance -> store-spec
        setup-schema (fn []
                       (let [store-spec (setup)
                             s (ddb/mk-schema store-spec)]
                         (swap! store-specs assoc s store-spec)
                         s))
        teardown-schema (fn [s]
                          (let [store-spec (@store-specs s)]
                            (teardown store-spec)))
        num-datasets 2]
    (schema/test-schema-properties-single-threaded
      setup-schema teardown-schema num-datasets)
    (schema/test-schema-properties-multi-threaded
      setup-schema teardown-schema num-datasets)))

(deftest test-dynamodb-mk-schema
  (testing
    "mk-schema function instantiates AWS credentials"
    (let [real-provider (DefaultAWSCredentialsProviderChain.)
          spy-state (atom {:get-credentials 0 :refresh 0})
          spy-provider (reify AWSCredentialsProvider
                         (getCredentials [this]
                           (swap! spy-state update-in [:get-credentials] inc)
                           (.getCredentials real-provider))
                         (refresh [this]
                           (swap! spy-state update-in [:refresh] inc)
                           (.refresh real-provider)))
          store-spec (assoc (setup) :provider spy-provider)]
      ; Create an empty dataset
      (.create-dataset (ddb/mk-schema store-spec) (:dataset store-spec))
      ; Check that getCredentials method was called
      (is (pos? (:get-credentials @spy-state)))))
  (testing
    "mk-schema function passes client configuration entries from store spec"
    (let [store-spec (assoc (setup)
                            :conn-timeout 1234
                            :socket-timeout 8765
                            :max-error-retry 7
                            :foo :bar)
          schema (ddb/mk-schema store-spec)
          dataset (:dataset store-spec)
          db-client (deref #'far/db-client)]
      (with-redefs [far/db-client (fn [client-opts]
                                    (is (= (dissoc store-spec :provider)
                                           (dissoc client-opts :provider))
                                        (str "mk-schema function preserves "
                                             "client configuration options"))
                                    (db-client client-opts))]
        ; Create an empty dataset
        (.create-dataset schema dataset)
        ; Read a chunk from this dataset
        (-> schema
          (.connect dataset)
          (.chunk-store {})
          (.read-chunk "nonexistentchunk"))))))

(deftest test-dynamodb-retry-backoff-defaults
  ; The purpose of this test is to future-proof against changes in
  ; defaults in future versions of AWS Java SDK
  (is (.isMaxErrorRetryInClientConfigHonored
        (PredefinedRetryPolicies/DYNAMODB_DEFAULT))
      (str "Default DynamoDB retry policy honors maximum error retry setting "
           "in the client configuration")))

(deftest* ddb-entire-flow
  (with-and-without-caches
    (entire-flow setup teardown)))

(deftest* ddb-grow-dataset
  (grow-dataset setup teardown))

(deftest* ddb-shrink-dataset
  (shrink-dataset setup teardown))

(deftest* ddb-overwrite-dataset
  (overwrite-dataset setup teardown))

(deftest* ddb-overwrite-extend-dataset
  (overwrite-extend-dataset setup teardown))

(deftest* ddb-linear-versions
  (linear-versions setup teardown))

(deftest* ddb-write-scalar
  (write-scalar setup teardown))

(deftest ^:integration ddb-lots-of-processes-ordered
  "This test can't be run locally yet since it spawns
   processes which can't be made to use the local ddb instance"
  (lots-of-processes setup teardown false))

(deftest ^:integration ddb-lots-of-processes-misordered
  "This test can't be run locally yet since it spawns
   processes which can't be made to use the local ddb instance"
  (lots-of-processes setup teardown true))

(deftest* ddb-lots-of-tiny-slices
  (with-and-without-caches
    (lots-of-tiny-slices setup teardown)))

(deftest* ddb-failed-write
  (failed-write setup teardown))

(deftest* ddb-lots-of-overlaps
  (with-and-without-caches
    (lots-of-overlaps setup teardown)))

(deftest* ddb-nan-fill-values
  (fill-double setup teardown)
  (fill-float setup teardown)
  (fill-short setup teardown))

(deftest* check-dynamodb-items-after-writes
  (with-temp-db store-spec setup teardown
    (testing
      "write-chunk method of DynamoDBChunkStore writes expected chunk bytes"
      (let [chunk-store (-> store-spec
                          (impl/mk-schema)
                          (.connect (:dataset store-spec))
                          (.chunk-store {}))
            expected-bytes (let [r (java.util.Random.)
                                 ba (byte-array 100)]
                             (.nextBytes r ba)
                             (ByteBuffer/wrap ba))
            sha (DigestUtils/shaHex (.array expected-bytes))
            expected-ref-count 10
            ; Directly construct a AmazonDynamoDBClient instance
            {:keys [provider
                    endpoint
                    access-key
                    secret-key]} (#'io.mandoline.backend.dynamodb/store-spec->client-opts
                                     store-spec)
            ddb-client (cond-> (if provider
                                 (AmazonDynamoDBClient. provider)
                                 (AmazonDynamoDBClient.
                                   (BasicAWSCredentials. access-key secret-key)))
                         endpoint (doto (.setEndpoint endpoint)))
            {:keys [dataset root]} store-spec
            table (-> (format "chunks.%s.%s" dataset root)
                    (split #"\.")
                    (reverse)
                    (#(join \. %)))
            item-request (doto (GetItemRequest.)
                           (.setTableName table)
                           (.setKey {"k" (doto (AttributeValue.) (.setS sha))})
                           (.setConsistentRead true))
            ; GetItem call using AWS SDK
            get-item #(.getItem (.getItem ddb-client item-request))]
        ; Call write-chunk method of DynamoDBChunkStore
        (.write-chunk chunk-store sha expected-ref-count expected-bytes)
        ; Call read-chunk method to read back the chunk
        (is (= expected-bytes (.read-chunk chunk-store sha)))
        ; Use AWS SDK to directly read the stored item from DynamoDB and
        ; compare its attribute values to expectations.
        (is (= expected-bytes
               (-> (get-item) (get "v") (.getB)))
            "write-chunk writes a DynamoDB item with expected chunk bytes")
        (is (= expected-ref-count
               (-> (get-item) (get "r") (.getN) (Long/parseLong)))
            "write-chunk writes a DynamoDB item with expected ref count")))))

(deftest test-lazy-query-pagination
  (let [query-func (fn [_ _ _ opts]
                     (case (opts :last-prim-kvs)
                       ; lazy-query looks for the :last-prim-kvs in the metadata
                       nil (with-meta [1 2 3] {:last-prim-kvs 3})
                       3 (with-meta [4 5 6] {:last-prim-kvs 6})
                       6 (with-meta [7 8 9] {:last-prim-kvs 9})))]
    (is (= [1 2 3 4 5 6 7 8 9]
           (take 9 (#'io.mandoline.backend.dynamodb/lazy-query query-func
                                                                       :not-needed
                                                                       :not-needed
                                                                       :not-needed
                                                                       {}))))))

(deftest ^:unit test-with-retry
  (testing "with-retry properly retries"
    (let [num-of-tries (atom 0)
          foo (fn []
                (when (> 5 @num-of-tries)
                  (do (swap! num-of-tries inc)
                      (throw (ProvisionedThroughputExceededException. "Try again.")))))]
      (is (nil? (ddb/with-retry 1 foo)))
      (is (= @num-of-tries 5)))))
