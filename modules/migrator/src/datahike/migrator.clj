(ns datahike.migrator
  (:require [datahike.api :as api]
            [datahike.datom :as d]
            [clojure.java.io :as io]
            [datahike.constants :as c]
            [clj-cbor.core :as cbor]))

(def allowed-formats #{:plain :cbor})

(defmulti write-file (fn [path data format] format))

(defmethod write-file :cbor [path data _]
  (cbor/spit-all path data))

(defmethod write-file :plain [path data _]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d data]
        (prn d)))))

(defmethod write-file :default [_ _ _]
  (throw (ex-info "Export format not available." {:datahike.export/error :format-not-available})))

(defn export-db
  "Export the database in a flat-file of datoms at path.
  Intended as a temporary solution, pending developments in Wanderung."
  ([conn path]
   (export-db conn path {:format :cbor}))
  ([conn path {:keys [format]}]
   (let [db @conn
         cfg (:config db)
         data (cond->> (sort-by
                        (juxt d/datom-tx :e)
                        (api/datoms (if (:keep-history? cfg) (api/history db) db) :eavt))
                (:attribute-refs? cfg) (remove #(= (d/datom-tx %) c/tx0))
                true (map seq))]
     (write-file path data format)
     true)))

(defn update-max-tx
  "Find bigest tx in datoms and update max-tx of db.
  Note: the last tx might not be the biggest if the db
  has been imported before."
  [db datoms]
  (assoc db :max-tx (reduce #(max %1 (nth %2 3)) 0 datoms)))

(defn- instance-to-date [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defmulti read-file (fn [path format] format))

(defmethod read-file :cbor [path _]
  (cbor/slurp-all path))

(defmethod read-file :plain [path _]
  (->> (line-seq (io/reader path))
       (map read-string)))

(defmethod read-file :default [_ export-format]
  (ex-info (format "Import format %s not available" export-format) {:datahike.import/error :format-not-available}))

(defn import-db
  "Import a flat-file of datoms at path into your database.
  Intended as a temporary solution, pending developments in Wanderung."

  ([conn path]
   (import-db conn path {:format :cbor}))
  ([conn path {:keys [format]}]
   (let [datoms (->> (read-file path format)
                     (map #(-> (apply d/datom %) (update :v instance-to-date))))]
     (swap! conn update-max-tx datoms)
     (print "Importing ")
     (api/transact conn (vec datoms))
     true)))


(comment

  (def cfg {:store {:backend :file :path "/tmp/foobar"}
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

  (api/delete-database cfg)
  (api/create-database cfg)

  (def conn (api/connect cfg))

  (api/transact conn default-schema)

  (let [size 100
        tx-size 10
        tx-count (-> (/ size tx-size)
                     Math/ceil
                     long)]
    (time (doseq [i (range tx-count )]
            (let [from (* tx-size i)
                  to (* tx-size (inc i))
                  tx-data (mapv (fn [j]
                                  {:page/title (str "T" j)
                                   :page/counter j
                                   :page/text (format "TXT%sTXT" j)
                                   :page/tags [:tag1 :tag2 :tag3]})
                                (range from (if (< size to) size to)))]
              (api/transact conn {:tx-data tx-data})))))

  (time (export-db conn "/tmp/foobar.plain" {:format :plain}))
  (time (export-db conn "/tmp/foobar.cbor" {:format :cbor}))

  (def cfg2 (assoc-in cfg [:store :path] "/tmp/foobar2"))

  (api/delete-database cfg2)
  (api/create-database cfg2)

  (def conn2 (api/connect cfg2))

  (time (import-db conn2 "/tmp/foobar.cbor" {:format :cbor}))

  (taoensso.timbre/set-level! :info)

  )




