(ns io.mandoline.backend.dynamodb
  (:require
    [clojure.core.memoize :as memo]
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [taoensso.encore :refer [doto-cond]]
    [taoensso.faraday :as far]
    [taoensso.faraday.utils :as far-utils]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.net URI]
    [java.util UUID]
    [java.nio ByteBuffer]
    [org.joda.time DateTime]
    [com.amazonaws AmazonServiceException]
    [com.amazonaws.auth
     DefaultAWSCredentialsProviderChain
     STSAssumeRoleSessionCredentialsProvider]
    [com.amazonaws.services.dynamodbv2.model
     AttributeValue
     ConditionalCheckFailedException
     GetItemRequest
     GetItemResult
     LimitExceededException
     PutItemRequest
     PutItemResult
     ResourceInUseException
     ResourceNotFoundException]))

;;;;;;;;;;;;;;;; DynamoDB schema ;;;;;;;;;;;;;;;;
;;
;; Chunk:   {:k <chunk-id (string)>
;;           :r <ref count (number)>
;;           :v <data (bytes)>}
;; Index:   {:k <var-name(string)|coordinate(strings joined by /)>
;;           :c <version-id (number)>
;;           :v <chunk id (string)>}
;; Version: {:k <dataset-id (string)>
;;           :t <version-id (number)>
;;           :v <json (string)>}
;;
;;TODO: change index hash key format to use something like
;;variable index, that will make index smaller
;;
;;;;;;;;;;;;;;;;;; Table names ;;;;;;;;;;;;;;;;;;
;;
;; Chunk:   <namespace>.<dataset-name>.chunks
;; Index:   <namespace>.<dataset-name>.indices
;; Version: <namespace>.<dataset-name>.versions
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def ^:dynamic *dynamodb-backoff-base-ms*
  "Time (in milliseconds) to wait between retries of a DynamoDB request that
  fails due to a ProvisionedThroughputExceededException.

  You can bind this var to tune the retry rate. The root binding is 50ms."
  50)

(def ^:dynamic *wait-ms*
  "Time (in milliseconds) to wait between retries of a DynamoDB request
  that transiently fails due to throttling or eventual consistency.

  You can bind this var to tune the retry rate. The root binding is 5000
  (= 5 seconds)."
  5000)

(def ^:dynamic *default-throughputs*
  {:chunks {:read 1000 :write 500}
   :indices {:read 250 :write 100}
   :versions {:read 20 :write 10}})

(defn with-retry
  "Attempt the provided function. If an exception is thrown due to AWS
  throttling, retry 'f' with randomized exponential backoff, starting in the
  range [ms, ms + 50%] milliseconds and increasing by 50% each time."
  [ms f & args]
  (try
    (apply f args)
    (catch Exception e
      ;; Catch the exception in both of com.amazonaws.services.dynamodb.{,v2}
      (if (re-find #"ProvisionedThroughputExceededException" (-> e .getClass .getName))
        (do
          (let [ms* (+ ms (rand-int (/ ms 2)))]
            (log/errorf "retrying call to %s in %d milliseconds" f (long ms*))
            (Thread/sleep (long ms*)))
          (apply with-retry (* ms 1.5) f args))
        (throw e)))))

(defn- list-tables
  [client-opts]
  (with-retry *dynamodb-backoff-base-ms* far/list-tables client-opts))

(defn describe-table
  [client-opts table]
  (with-retry *dynamodb-backoff-base-ms* far/describe-table client-opts table))

(defn- table-exists?
  [client-opts table]
  (not (nil? (describe-table client-opts table))))

(defn- db-val->clj-val*binary-safe
  "Similar to the private function taoensso.faraday/db-val->clj-val,
  except that it does not attempt to Nippy-thaw binary attributes."
  [^AttributeValue x]
  (or (.getS x)
      (some->> (.getN  x) (#'far/ddb-num-str->num))
      (some->> (.getB  x) (identity)) ; do not attempt to thaw binary data
      (some->> (.getSS x) (into #{}))
      (some->> (.getNS x) (mapv #'far/ddb-num-str->num) (into #{}))
      (some->> (.getBS x) (mapv identity) (into #{})))) ; do not attempt to thaw binary data

(defn- db-item->clj-item*binary-safe
  "Similar to the function taoensso.faraday/db-item->clj-item, except
  that it does not attempt to deserialize binary attributes."
  [item]
  (far-utils/keyword-map db-val->clj-val*binary-safe item))

(defn- clj-val->db-val*binary-safe
  "Similar to the private function taoensso.faraday/clj-val->db-val,
  except that it does not attempt to Nippy-freeze a ByteBuffer
  instance."
  ^AttributeValue [x]
  (if (instance? ByteBuffer x)
    (doto (AttributeValue.) (.setB x))
    (#'far/clj-val->db-val x)))

(defn- clj-item->db-item*binary-safe
  "Similar to the function taoensso.faraday/clj-item->db-item, except
  that it does not attempt to serialize binary attributes."
  [item]
  (far-utils/name-map clj-val->db-val*binary-safe item))

(defmulti as-map*binary-safe
  "Similar to the implementation of the taoensso.faraday/as-map protocol
  method for GetItemResult and PutItemResult, but customized to handle
  binary attributes as-is without attempting Nippy
  serialization/deserialization."
  {:private true}
  class)

(defmethod as-map*binary-safe GetItemResult
  [x]
  (when x
    (with-meta (db-item->clj-item*binary-safe (.getItem x))
      {:cc-units (some-> (.getConsumedCapacity x) (.getCapacityUnits))})))

(defmethod as-map*binary-safe PutItemResult
  [x]
  (when x
    (with-meta (db-item->clj-item*binary-safe (.getAttributes x))
      {:cc-units (some-> (.getConsumedCapacity x) (.getCapacityUnits))})))

(defn- get-item*binary-safe
  "Similar to the function taoensso.faraday/get-item, except that it
  faithfully returns binary attributes directly as ByteBuffer instances,
  bypassing Nippy deserialization."
  [client-opts table prim-kvs & [{:keys [attrs consistent? return-cc?]}]]
  (as-map*binary-safe
    (.getItem (#'far/db-client client-opts)
              (doto-cond [g (GetItemRequest.)]
                         :always     (.setTableName       (name table))
                         :always     (.setKey             (clj-item->db-item*binary-safe prim-kvs))
                         consistent? (.setConsistentRead  g)
                         attrs       (.setAttributesToGet (mapv name g))
                         return-cc?  (.setReturnConsumedCapacity (far-utils/enum :total))))))

(defn- put-item*binary-safe
  "Similar to the function taoensso.faraday/put-item, except that it
  faithfully stores ByteBuffer instances directly as binary attributes,
  bypassing Nippy serialization."
  [client-opts table item & [{:keys [return expected return-cc?]
                              :or   {return :none}}]]
  (as-map*binary-safe
    (.putItem (#'far/db-client client-opts)
              (doto-cond [g (PutItemRequest.)]
                         :always  (.setTableName    (name table))
                         :always  (.setItem         (clj-item->db-item*binary-safe item))
                         expected (.setExpected     (#'far/expected-values g))
                         return   (.setReturnValues (far-utils/enum g))
                         return-cc? (.setReturnConsumedCapacity (far-utils/enum :total))))))

(defn- query
  ; TODO binary-safe variant of the query function
  [client-opts table prim-key-conds & opts]
  (apply far/query client-opts table prim-key-conds opts))

(defn create-table
  "Make a CreateTable request.

  The default behavior of this function is to poll until the table has
  ACTIVE status. To override this default behavior, provide the keyword
  argument `:block? false`."
  [client-opts table hash-keydef & opts]
  (let [opts* (if (some #{:block?} opts)
                opts
                (concat opts [:block? true]))
        create! (fn []
                  (try
                    (far/create-table
                      client-opts
                      table
                      hash-keydef
                      opts*) :success
                    (catch LimitExceededException _
                      nil)))]
    (while (not (create!))
      (Thread/sleep *wait-ms*))))

(defn delete-table
  "Make a DeleteTable request and poll until the table no longer
  exists."
  [client-opts table]
  (letfn [(delete! []
            (try
              (far/delete-table client-opts table)
              :success
              (catch LimitExceededException _
                nil)
              (catch ResourceInUseException _
                nil)
              (catch ResourceNotFoundException _
                :success)))]
    (while (not (delete!))
      (Thread/sleep *wait-ms*)))
  (while (table-exists? client-opts table)
    (Thread/sleep *wait-ms*)))

(defn get-table-name
  "Construct a table name from one or more table name components.
  Keywords are automatically converted to their string names.

  (get-table-name \"com.foo.dev.bar\" \"datasetname\"))
  => \"com.foo.dev.bar.datasetname\"

  (get-table-name \"com.foo.dev.bar\" :datasetname))
  => \"com.foo.dev.bar.datasetname\"
  "
  [head & more]
  (string/join \. (map name (cons head more))))

(defn table-stats [client-opts table]
  "Gets statistics on a DynamoDB table."
  (if-let [desc (describe-table client-opts table)]
    (:table-size-bytes desc)
    (throw
      (ResourceNotFoundException.
        (str "Could not describe DynamoDB table" (name table))))))

(defn- find-index
  "Recursively try to find the index coordinate in a committed version.
  op is either :lt, :le or :eq"
  [version-cache client-opts table version-id [hash-key range-key] op]
  (when-let [row (-> (query
                       client-opts
                       (get-table-name table "indices")
                       {:k [:eq hash-key]
                        :c [op range-key]}
                       {:return [:v :c]
                        :span-reqs nil
                        :order :desc
                        :limit 1})
                   first)]
    (let [version (get row :c)]
      (if (or (= version version-id) (utils/committed? version-cache version))
        (get row :v)
        (recur version-cache client-opts table version-id
               [hash-key version] :lt)))))

(defn- coordinate->id [coord]
  (if (empty? coord)
    "_"
    (->> coord (interpose "/") (apply str))))

(defn- coordinate->key [metadata var-name coord]
  [(str (name var-name) "|" (coordinate->id coord))
   (:version-id metadata)])

(letfn [(metadata [client-opts table version]
          (get-item*binary-safe
            client-opts
            (get-table-name table "versions")
            {:k "v" :t (Long/parseLong version)}
            {:consistent? true
             :attrs [:v]}))]
  (def get-metadata (memo/lru metadata :lru/threshold 500)))

(deftype DynamoDBIndex [table client-opts var-name metadata version-cache]
  ;; table is a dataset-level table name
  proto/Index

  (target [_] {:metadata metadata :var-name var-name})

  (chunk-at [_ coordinate]
    (find-index
      version-cache client-opts table
      (:version-id metadata)
      (coordinate->key metadata var-name coordinate)
      :le))

  (chunk-at [_ coordinate version-id]
    (find-index
      version-cache client-opts table
      version-id
      (coordinate->key metadata var-name coordinate)
      :eq))

  (write-index [_ coordinate old-hash new-hash]
    (let [key (coordinate->key metadata var-name coordinate)]
      (when (not= "" (last key))
        (log/debugf "writing index at coordinate: %s" (pr-str coordinate))
        (try
          (put-item*binary-safe
            client-opts
            (get-table-name table "indices")
            {:k (first key)
             :c (last key)
             :v new-hash}
            {:expected (if (nil? old-hash)
                         {:v false}
                         {:c (last key) :v old-hash})})
          true
          (catch ConditionalCheckFailedException _
            false)))))

  (flush-index [_]
    (log/debugf "DynamoDBIndex flush method called: no-op")))

(defn- read-a-chunk [client-opts table hash consistent?]
  (when (or (empty? hash) (not (string? hash)))
    (throw
      (IllegalArgumentException. "hash must be a non-empty string")))
  (log/debugf
    "Reading chunk %s with %s consistency"
    hash
    (if consistent? "strong" "eventual"))
  (let [item (get-item*binary-safe
               client-opts
               (get-table-name table "chunks")
               {:k hash}
               {:consistent? consistent?
                :attrs [:k :v :r]})]
    (when (seq item) item)))

(deftype DynamoDBChunkStore [table client-opts]
  proto/ChunkStore

  (read-chunk [_ hash]
    ; Do an eventually consistent read first, and if that returns
    ; nothing, do a strongly consistent read.
    (if-let [item (or (read-a-chunk client-opts table hash false)
                      (do
                        (log/debugf
                          (str
                            "Eventually consistent read did not find item for "
                            "hash %s; retrying with strongly consistent read.")
                          hash)
                        (read-a-chunk client-opts table hash true)))]
      (:v item)
      (throw
        (IllegalArgumentException.
          (format "No chunk was found for hash %s" hash)))))

  (chunk-refs [_ hash]
    ; This method is expensive and may trigger throttling because it
    ; always performs a strongly consistent read.
    (if-let [item (read-a-chunk client-opts table hash true)]
      (long (:r item)) ; We expect reference counts to be in the range of long
      (throw
        (IllegalArgumentException.
          (format "No reference count was found for hash %s" hash)))))

  (write-chunk [_ hash ref-count bytes]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? ref-count)
      (throw
        (IllegalArgumentException. "ref-count must be an integer")))
    (when-not (instance? ByteBuffer bytes)
      (throw
        (IllegalArgumentException. "bytes must be a ByteBuffer instance")))
    (when-not (pos? (.remaining bytes))
      (throw
        (IllegalArgumentException. "Chunk has no remaining bytes")))
    (log/debugf "Writing chunk at hash: %s" hash)
    (put-item*binary-safe
      client-opts
      (get-table-name table "chunks")
      {:k hash
       :r ref-count
       :v bytes}
      {:return :none})
    nil)

  (update-chunk-refs [_ hash delta]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? delta)
      (throw
        (IllegalArgumentException. "delta must be an integer")))
    (loop [item (read-a-chunk client-opts table hash true)]
      (when-not item
        (throw
          (IllegalArgumentException.
            (format "No chunk was found for hash %s" hash))))
      (or
        (try
          (put-item*binary-safe
            client-opts
            (get-table-name table "chunks")
            (update-in item [:r] #(+ % delta))
            {:expected (select-keys item [:r])
             :return :none})
          (catch ConditionalCheckFailedException _
            false))
        (recur (read-a-chunk client-opts table hash true))))
    nil))

(defn- lazy-query
  "This function wraps the faraday dynamodb library's query function to allow
   for lazy pagination

   Params:

   query-func - faraday/query (or something that transparently wrap it)
   client-opts - the same client options you would pass to farday/query
   table-name - the same table name you would pass to farday/query
   prim-key-conds - the same primary key conditions you would pass to
   farday/query opts - the same opts you would pass to farday/query

   For more details on these params, see (doc farday/query)

   Returns:

   A lazy-seq of results
   "
  [query-func client-opts table-name prim-key-conds opts]
  (letfn [(step [prev-key]
            (lazy-seq (let [chunk
                            (query-func client-opts
                                        table-name
                                        prim-key-conds
                                        (conj opts
                                              (when prev-key
                                                [:last-prim-kvs prev-key])))
                            last-key (-> chunk meta :last-prim-kvs)]
                        (if last-key (concat chunk (step last-key)) chunk))))]
    (step nil)))


(deftype DynamoDBConnection [table client-opts]
  proto/Connection

  (index [this var-name metadata options]
    (->DynamoDBIndex table client-opts var-name metadata
                     (utils/mk-version-cache
                       (map #(-> % :version Long/parseLong)
                            (proto/versions this {:metadata? false})))))

  (write-version [_ metadata]
    ;; TODO: commit must fail if last version != parent-version
    (put-item*binary-safe
      client-opts
      (get-table-name table "versions")
      {:k "v"
       :t (:version-id metadata)
       :v (utils/generate-metadata metadata)}))

  (chunk-store [_ options]
    (->DynamoDBChunkStore table client-opts))

  (get-stats [_]
    {:metadata-size (table-stats client-opts (get-table-name table "versions"))
     :index-size (table-stats client-opts (get-table-name table "indices"))
     :data-size (table-stats client-opts (get-table-name table "chunks"))})

  (metadata [_ version]
    (-> (get-metadata client-opts table version)
        (get :v)
        (utils/parse-metadata true)))

  (versions [_ {:keys [limit metadata?]}]
    (let [limit-fn (if limit #(take limit %) #(identity %))
          expand-fn (fn [x]
                      (let [ms (get x :t)
                            metadata (-> (get x :v)
                                         (utils/parse-metadata true))]
                        (merge
                         {:timestamp (DateTime. (.longValue ms))
                          :version (str ms)}
                         (when metadata? {:metadata metadata}))))]
      (->> (lazy-query
             query
             client-opts
             (get-table-name table "versions")
             {:k [:eq "v"]}
             {:limit limit
              :return (if metadata? [:t :v] [:t])
              :span-reqs {:max 1}
              :order :desc})
           (limit-fn)
           (map expand-fn)))))

(deftype DynamoDBSchema [root-table client-opts]
  proto/Schema

  (create-dataset [_ name]
    (when-not (and (string? name) (not (string/blank? name)))
      (throw
        (IllegalArgumentException.
          "dataset name must be a non-empty string")))
    (let [root-path (get-table-name root-table name)]
      ; create-table throws ResourceInUseException if table already
      ; exists
      (create-table
        client-opts
        (get-table-name root-path "chunks")
        [:k :s]
        :throughput (:chunks *default-throughputs*))
      (create-table
        client-opts
        (get-table-name root-path "indices")
        [:k :s]
        :range-keydef [:c :n]
        :throughput (:indices *default-throughputs*))
      (create-table
        client-opts
        (get-table-name root-path "versions")
        [:k :s]
        :range-keydef [:t :n]
        :throughput (:versions *default-throughputs*)))
    nil)

  (destroy-dataset [_ name]
    (doseq [t ["versions" "indices" "chunks"]]
      (->> (get-table-name root-table name t)
        (delete-table client-opts))))

  (list-datasets [_]
    (let [prefix (str root-table ".")
          filter-fn (fn [t]
                      (.startsWith (name t) prefix))
          extract-fn (fn [t]
                       (->> (string/replace (name t) prefix "")
                            (#(string/split % #"[.]+"))
                            ;; drop table suffix
                            (first)))]
      (->> (list-tables client-opts)
        (filter filter-fn)
        (map extract-fn)
        (distinct))))

  (connect [_ dataset-name]
    (let [conn (->DynamoDBConnection
                 (get-table-name root-table dataset-name) client-opts)]
      (try ; Check that the connection is functional
        (.get-stats conn)
        (catch Exception e
          (throw
            (RuntimeException.
              (format
                "Failed to connect to dataset \"%s\" with root-table \"%s\""
                dataset-name root-table)
              e))))
      conn)))

(defn- random-session-name []
  (.substring (str (UUID/randomUUID)) 0 32))

(defn store-spec->client-opts
  "Given a Mandoline store spec map, returns a map of client
  configuration options that contains an object that implements
  AWSCredentialsProvider interface.

  The store-spec argument to this function must be a map. All entries in
  this map are optional. Recognized keyword entries are documented
  below. WARNING: The behavior of this function is undefined when the
  store spec contains keys other than these recognized keywords.

  This function returns a client configuration map that includes a
  :provider entry whose value is a AWSCredentialsProvider instance. All
  other entires from the store are copied into the returned client
  configuration.

    :provider
        An object that implements AWSCredentialsProvider interface. If
        the store spec includes this entry, then this function uses its
        value as a base credentials provider. If the store spec does not
        include a :provider entry, then this function instantiates a
        DefaultAWSCredentialsProviderChain, which looks for credentials
        in a well-defined order. See AWS documentation for this
        provider:
        http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html

    :role-arn
        A string; the Amazon Resource Name (ARN) for a IAM role. If the
        store spec includes a :role-arn entry, then this function
        instantiates a STSAssumeRoleSessionCredentialsProvider that uses
        the role identified by this ARN. If the store spec also includes
        a :provider entry, then the provider in the store spec is used
        as a long lasting credentials provider that is wrapped by the
        STSAssumeRoleSessionCredentialsProvider.

    :session-name
        A string; the name of the IAM role session. If the store spec
        includes this entry and also includes a :role-arn entry, then
        this function instantiates a
        STSAssumeRoleSessionCredentialsProvider whose role session name
        equals the value. Otherwise, this function generates a random
        role session name.

    :endpoint
        A string that represents an endpoint URL for making DynamoDB
        requests. If the store spec includes this entry, then this
        function overrides the default endpoint for the Amazon DynamoDB
        client. This entry is intended for testing; for example, if a
        test double is running on port 8000 on localhost, then an
        :endpoint value of \"http://localhost:8000\" will use the test
        double instead of the real Amazon DynamoDB service.

    :conn-timeout
        Non-negative integer value that is the amount of time to wait
        (in milliseconds) when initially establishing a connection
        before giving up and timing out. If the store spec does not
        include this entry, then the default value in the AWS Java SDK
        is assumed.

    :max-conns
        Non-negative integer value that is the maximum number of allowed
        open HTTP connections. If the store spec does not include this
        entry, then the default value in the AWS Java SDK is assumed.

    :max-error-retry
        Non-negative integer value that is the maximum number of retry
        attempts for failed retryable requests (e.g. exceptions due to
        throughput throttling). If the store spec does not include this
        entry, then the default value in the AWS Java SDK is assumed.

    :socket-timeout
        Non-negative integer that is the amount of time to wait (in
        milliseconds) for data to be transfered over an established,
        open connection before the connection times out and is closed.
        If the store spec does not include this entry, then the default
        value in the AWS Java SDK is assumed."
  [& [{:keys [provider role-arn session-name] :as store-spec}]]
  (let [session-name (or session-name (random-session-name))
        provider (cond
                  (and provider role-arn) (STSAssumeRoleSessionCredentialsProvider.
                                           provider
                                           role-arn
                                           session-name)
                  provider provider
                  role-arn (STSAssumeRoleSessionCredentialsProvider.
                            role-arn
                            session-name)
                  :else (DefaultAWSCredentialsProviderChain.))]
    (-> store-spec
      (dissoc :role-arn)
      (merge {:provider provider}))))

(defn root-table-prefix
  "Given a DynamoDBSchema root, construct a root prefix for naming the
  associated DynamoDB tables.

  This function reverses the components of the provided DynamoDBSchema
  root to construct the table naming prefix. Example:

      (root-table-prefix \"foo.bar.com\") => \"com.bar.foo\"
  "
  ([root db-version]
   (->> (string/split root #"[.]+")
     (reverse)
     (#(if db-version (cons db-version %) %))
     (string/join ".")))
  ([root]
   (root-table-prefix root nil)))

(defn mk-schema [store-spec]
  "Given a store spec map, return a DynamoDBSchema instance

  The store-spec argument is a map that can include the following entries

   :root            - the root of the store
   :db-version      - (optional) the version of this library
   :provider        - (optional) AWSCredentialsProvider to use
   :role-arn        - (optional) the IAM role to assume when accessing DynamoDB
   :session-name    - (optional) a unique identifier to id this role session
   :endpoint        - (optional) override endpoint URL for DynamoDB
   :conn-timeout    - (optional) timeout when opening connection
   :max-conns       - (optional) max number of HTTP connections
   :max-error-retry - (optional) max number of retries to attempt on recoverable failures
   :socket-timeout  - (optional) timeout for data transfer on an open connection

  For more details, see the docstring for the
  io.mandoline.dynamodb.backend/store-spec->client-opts function."
  (let [root-table (root-table-prefix
                     (:root store-spec) (:db-version store-spec))]
    (->DynamoDBSchema root-table (store-spec->client-opts store-spec))))
