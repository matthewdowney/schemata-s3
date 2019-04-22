(ns schemata-s3.core-test
  "Tests expect a 's3-context-test.key' file, containing an EDN config map.
  For example
    {:bucket   \"MyTestBucket\"
     :endpoint \"us-east-1\"
     ;; You can omit these if the aws cli is configured on the machine
     :access-key \"abcdef\"
     :secret-key \"abcdef\"}"
  (:require [clojure.test :refer :all]
            [schemata.core :as s]
            [schemata-s3.core :refer :all]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.edn :as edn]))

(defn get-test-config []
  (when (.isFile (io/file "s3-context-test.key"))
    (edn/read-string (slurp "s3-context-test.key"))))

(defn hide-prefix
  [s]
  (when s
    (let [n-showable (if (> (count s) 8) 4 (/ (count s) 2))]
      (str
        (apply str (repeat (- (count s) n-showable) "*"))
        (subs s (- (count s) n-showable) (count s))))))

(def mock-naming
  (s/path-convention
    :type
    (s/file-convention
      (s/split-by "_" :name (s/utc :ts "yyyy-MM-dd"))
      "log")))

(def mock-root
  "schemata-s3-test")

(defn contexts [config]
  {:local (s/local-context mock-root mock-naming)
   :s3    (s3-context
            (dissoc config :bucket) (:bucket config)
            mock-naming mock-root)})

(defn test-files [config]
  (let [{:keys [local s3]} (contexts config)]
    {:local {:context  local
             :spec     {:ts 1555804800000 :type "ticker" :name "Bitstamp"}
             :resolved (.getCanonicalPath
                         (io/file mock-root "ticker" "Bitstamp_2019-04-21.log"))
             :desc     "local context"}
     :s3    {:context  s3
             :spec     {:ts 1555891200000 :type "ticker" :name "Bitstamp"}
             :resolved (cond-> {:bucket (:bucket config)
                                :key    (str mock-root "/ticker/Bitstamp_2019-04-22.log")}
                               (some? (:endpoint config))
                               (assoc :endpoint (:endpoint config)))
             :desc     "s3 context"}}))

(deftest s3-context-test
  (let [config (get-test-config)]
    ;; Make sure the test configuration is okay
    (testing "Have valid test configuration"
      (is (map? config))
      (is (some? (:bucket config))))

    (when (and (map? config) (:bucket config))
      (println "Using config:")
      (pprint/pprint
        (-> config
            (update :access-key hide-prefix)
            (update :secret-key hide-prefix)))

      (let [{:keys [local s3]} (test-files config)]
        ;; Run one test that's local <-> s3 and another reversed
        (s/test-with local s3)
        (s/test-with s3 local)))))

(comment
  (run-tests))
