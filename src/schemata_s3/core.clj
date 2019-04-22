(ns schemata-s3.core
  ^{:doc "Implementation of schemata for Amazon S3 using
          https://github.com/mcohen01/amazonica."
    :author "Matthew Downey"}
  (:require [schemata.core :as s]
            [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import (java.io ByteArrayOutputStream ByteArrayInputStream)
           (org.joda.time DateTime)
           (schemata.core LocalContext)))

;;; S3's IOFactory

(defn- byte-output-stream
  "A byte output stream that calls the `on-close` handler with a byte array
  upon close."
  [on-close]
  (proxy [ByteArrayOutputStream] []
    (close []
      (on-close (.toByteArray this))
      (proxy-super close))))

(defrecord S3IOFactory [config bucket key-name])

(extend S3IOFactory
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream
    (fn [{:keys [config bucket key-name]} opts]
      (-> (s3/get-object config {:bucket-name bucket :key key-name})
          :object-content))

    :make-output-stream
    (fn [{:keys [config bucket key-name] :as this} opts]
      (let [;; An output stream that flushes by uploading to S3 only after close
            os (byte-output-stream
                 (fn [bytes-array]
                   (s3/put-object
                     config
                     {:bucket-name   bucket
                      :key           key-name
                      :input-stream  (ByteArrayInputStream. bytes-array)
                      :metadata      {:content-length (count bytes-array)}
                      :return-values "ALL_OLD"})))]

        ;; If they want to append, first write the current vals to the output
        ;; stream
        (when (:append opts)
          (io/copy (io/input-stream this) os))
        os))))

(defn s3-io-factory
  "An implementation of clojure.java.io/IOFactory for some S3 object."
  [config bucket key-name]
  (->S3IOFactory config bucket key-name))

;;; "ls" implementation -- amazonica makes this quite manual

(defn- list-objects
  "Returns a lazy sequence of objects, performing pagination automatically,
  for the same arguments as `s3/list-objects-v2`.

  There are lots of objects, so specifying a :prefix is recommended."
  ([config request]
   (map ;; Give each of 'em a :base-name and convert the joda time to ms
     (fn [datum]
       (-> datum
           (assoc :base-name (last (string/split (:key datum) #"/")))
           (update :last-modified #(.getMillis ^DateTime %))))
     (list-objects config request nil)))
  ([config request page-id]
   (lazy-seq
     (let [request' (cond-> request
                            (some? page-id) (assoc :continuation-token page-id))
           resp (s3/list-objects-v2 config request)]
       ;; If it returned the max amount of objects, there might be more to
       ;; paginate through.
       (if (= (count (:object-summaries resp)) (:max-keys resp))
         (lazy-cat
           (:object-summaries resp)
           (list-objects config request' (:next-continuation-token resp)))
         (:object-summaries resp))))))

;;; Context implementation

(defn- s3-join
  "Like (io/file parent child & more) except is guaranteed to use '/'
  as the separator, regardless of the value of File/separator (maybe
  you're running on a Windows host?)."
  [parent child & more]
  (let [parts (cons parent (cons child more))
        trim-part (fn [part]
                    ;; Trim the start, if there are any leading '/' chars
                    (let [part' (cond-> part
                                        (string/starts-with? part "/") (subs 1))]
                      (if (string/ends-with? part "/")
                        (subs part' 0 (dec (count part')))
                        part')))]
    (string/join "/" (map trim-part parts))))

(defn- ->key-name [root naming-convention spec]
  (let [path-parts (cond->> (s/spec->path naming-convention spec)
                            (some? root) (cons root))]
    (apply s3-join path-parts)))

(defrecord S3Context [config bucket root naming-convention]

  s/Context
  (io [this spec]
    (s3-io-factory config bucket (->key-name root naming-convention spec)))

  (resolve [this spec]
    (cond->
      {:bucket bucket :key (->key-name root naming-convention spec)}
      (contains? config :endpoint) (assoc :endpoint (:endpoint config))))

  (delete [this]
    (let [things-under-root (try
                              (into [] (s/list this {:strict? true}))
                              (catch Throwable t t))]
      ;; When there are not file items under the root, recursively delete
      ;; the directory structure
      (cond
        (instance? Exception things-under-root)
        (-> "Couldn't verify that context was empty before deletion."
            (ex-info {})
            (throw))

        (not-empty things-under-root)
        (-> "Cannot delete context; it's not empty."
            (ex-info {:contains things-under-root})
            (throw))

        :else
        (doseq [dir (list-objects
                      config
                      (cond-> {:bucket-name bucket}
                              (some? root) (assoc :prefix root)))]
          (s3/delete-object
            config
            :bucket-name bucket
            :key (:key dir))))))

  (delete [this spec]
    (s3/delete-object
      config
      :bucket-name bucket
      :key (->key-name root naming-convention spec)))

  (info [this spec]
    (if-let [info? (::info spec)]
      info?
      (let [key-name (->key-name root naming-convention spec)
            queried (list-objects config {:bucket-name bucket :prefix key-name})]
        ;; There might be other objects that start with that key
        (if-let [summary (some #(when (= (:key %) key-name) %) queried)]
          summary
          ;; Otherwise it doesn't exist
          {:size 0, :last-modified 0}))))

  (list [this]
    (s/list this {}))

  (list [this {:keys [strict?] :as opts}]
    ;; On S3, directories are keys with size = 0
    (let [is-directory? (comp zero? :size)
          read-bucket-obj (fn [{:keys [key] :as bucket-obj}]
                            (let [path (string/split key #"/")
                                  spec (s/path->spec naming-convention path)]
                              ;; It'd be a shame to waste all that info... stick
                              ;; it innocuously in the returned spec
                              (cond-> spec (map? spec) (assoc ::info bucket-obj))))]
      (sequence
        (comp
          (filter (complement is-directory?)) ;; Exclude "directories"
          (map #(try (read-bucket-obj %)
                     (catch Exception e (when strict? (throw e)))))
          (filter some?))
        (list-objects
          config
          (cond-> {:bucket-name bucket} (some? root) (assoc :prefix root)))))))

(defn default-naming-convention
  "Create a naming convention for the bucket that works with path
  literals, e.g. 's3://my-bucket/some-directory/file.txt'."
  [bucket]
  (let [pfx (format "s3://%s/" bucket)]
    (reify s/NamingConvention
      (spec->path [_ path]
        (when-not (string/starts-with? path pfx)
          (throw (ex-info "Invalid path" {:expected-prefix pfx :given-path path})))
        (-> (string/split path #"/") rest rest rest))
      (path->spec [_ path]
        (str pfx (string/join "/" path))))))

(defn s3-context
  "Create a `schemata.core/Context` implementation for S3 buckets.

  Options
    - config    Config map for Amazonica S3, which can include an :endpoint
                (like \"us-east-1\"), :access-key, and :secret-key.
                (See https://github.com/mcohen01/amazonica#authentication).
    - bucket    The S3 bucket name.
    - root      An optional path from the bucket root to the directory under
                which the naming convention applies (can be . or nil)."
  ([config bucket]
   (s3-context config bucket (default-naming-convention bucket)))
  ([config bucket naming-convention]
   (s3-context config bucket naming-convention nil))
  ([config bucket naming-convention root]
   (let [root' (when (and (some? root) (not= "." root))
                 root)]
     (->S3Context config bucket root' naming-convention))))

;; We can do better on file copying from local to S3 by using the :file keyword
(defmethod s/copy [LocalContext S3Context]
  [from-spec to-spec from-file-context to-file-context]
  (s3/put-object
    (:config to-file-context)
    {:bucket-name (:bucket to-file-context)
     :key         (->key-name
                    (:root to-file-context)
                    (:naming-convention to-file-context)
                    to-spec)
     :file        (s/io from-file-context from-spec)}))
