(ns datahike.migrator-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [datahike.migrator :as m]
            [clojure.java.io :as io]
            [datahike.api :as api]))

(def cfg {:store {:backend :file
                  :path (format "%s/datahike_migrator_test" (System/getProperty "java.io.tmpdir"))}
          :keep-history? true
          :schema-flexibility :write
          :attribute-refs? false})

(def default-schema [{:db/ident       :page/title
                      :db/cardinality :db.cardinality/one
                      :db/unique :db.unique/identity
                      :db/valueType   :db.type/string}
                     {:db/ident :page/counter
                      :db/cardinality :db.cardinality/one
                      :db/valueType :db.type/long}
                     {:db/ident :page/text
                      :db/cardinality :db.cardinality/one
                      :db/valueType :db.type/string}
                     {:db/valueType :db.type/keyword
                      :db/ident :page/tags
                      :db/cardinality :db.cardinality/many}])

(def cbor-export-file (format "%s/datahike_migrator.cbor" (System/getProperty "java.io.tmpdir")))
(def plain-export-file (format "%s/datahike_migrator.plain" (System/getProperty "java.io.tmpdir")))

(defn setup []
  (api/delete-database cfg)
  (api/create-database cfg)
  (let [conn (api/connect cfg)
        size 100
        tx-size 10
        tx-count (-> (/ size tx-size)
                     Math/ceil
                     long)]
    (api/transact conn default-schema)
    (doseq [i (range tx-count )]
      (let [from (* tx-size i)
            to (* tx-size (inc i))
            tx-data (mapv (fn [j]
                            {:page/title (str "T" j)
                             :page/counter j
                             :page/text (format "TXT%sTXT" j)
                             :page/tags [:tag1 :tag2 :tag3]})
                          (range from (if (< size to) size to)))]
        (api/transact conn {:tx-data tx-data})))))

(defn teardown []
  (api/delete-database cfg)
  (when (.exists (io/file cbor-export-file))
    (io/delete-file cbor-export-file))
  (when (.exists (io/file plain-export-file))
    (io/delete-file plain-export-file)))

(defn with-test-db [f]
  (setup)
  (f)
  (teardown))

(use-fixtures :once with-test-db)


(deftest cbor-test
  (testing "export succeeds")
  (testing "import succeeds")
  (testing "import db has the correct data"))

(deftest plain-test
  (testing "export succeeds")
  (testing "import succeeds")
  (testing "import db has the correct data"))

(deftest error-test
  (testing "unknown method"))
