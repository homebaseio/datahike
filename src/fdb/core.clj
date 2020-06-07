(ns fdb.core
  (:import (com.apple.foundationdb FDB
                                   Transaction
                                   Range
                                   KeySelector)
           (java.util List))
  (:require [fdb.keys :refer [key ->byteArr key->vect max-key]]))

(defmacro tr!
  "Transaction macro to perform actions. Always use tr for actions inside
  each action since the transaction variable is bound to tr in the functions."
  [db & actions]
  `(.run ~db
         (reify
           java.util.function.Function
           (apply [this tr]
             ~@actions))))


(def api-version 510)

(defn db
  []
  (let [fd (FDB/selectAPIVersion api-version)]
    (with-open [db (.open fd)]
      db)))


;; TODO: Currently there is only one and only one instance of FDB ever created.
;; I.e. I can't create multiple instances of the db with giving each a name for instance.
(defn empty-db
  "Clear all keys from the database. Thus returns an empty db."
  []
  (let [fd    (FDB/selectAPIVersion api-version)
        begin (byte-array [])
        end   (byte-array [0xFF])]
    (with-open [db (.open fd)]
      (tr! db (.clear tr (Range. begin end)))
      db)))


(defn clear-all []
  (empty-db))


;; TODO?: remove the db arg
(defn get
  [db index-type [e a v t]]
  (let [fd  (FDB/selectAPIVersion api-version)
        key (key index-type [e a v t])]
    (with-open [db (.open fd)]
      (tr! db @(.get tr key)))))


(defn insert
  "Inserts one vector"
  [index-type [e a v t]]
  (let [fd    (FDB/selectAPIVersion api-version)
        key   (key index-type [e a v t])
        ;; The value is also the key
        value key]
(println "Insert: " index-type " - " (key->vect index-type key))
    (with-open [db (.open fd)]
      (tr! db (.set tr key value))
      db)))


(defn batch-insert
  "Batch inserts multiple vectors. `index-type` is :eavt, etc..."
  [index-type vectors]
  (let [fd   (FDB/selectAPIVersion api-version)
        keys (map #(key index-type %) vectors)
        v    (byte-array [])]
    (with-open [db (.open fd)]
      ;; The value 5000 depends on the size of a fdb key.
      ;; I.e. We have to find a combination such that ~
      ;; 5000 * <fdb key size> does not exceed 10MB (the transaction max size
      ;; for fdb).
      (doall (doseq [some_vecs (partition 5000 keys)]
               (tr! db (doseq [k some_vecs]
                         ;; (println k)
                         (.set tr k v))))))))

;; What is sent by Datahike to 'getRange' (depending on type of the query)?
(comment
  (def conn (d/create-conn {:aka { :db/cardinality :db.cardinality/many }}))

  (do
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka  "Devil"]])
    (d/transact! conn [[:db/add 1 :aka  "Tupen"]]))

  ;; :eavt  [1 :name nil 536870912 true] ----  [1 :name nil 2147483647 true]
  (d/q '[:find ?v
         :where [1 :name ?v]] @conn)

  ;; :aevt  --   [0 :name nil 536870912 true] ----  [2147483647 :name nil 2147483647 true]
  (d/q '[:find ?e
         :where [?e :name "Ivan"]] @conn)

  ;; :eavt  --   [1 nil nil 536870912 true] ----  [1 nil nil 2147483647 true]
  (d/q '[:find ?a
         :where [1 ?a _]] @conn)

  ;; :eavt  --   [1 nil nil 536870912 true] ----  [1 nil nil 2147483647 true]
  (d/q '[:find ?a
         :where [1 ?a "Ivan"]] @conn)

  ;; :aevt  --   [0 :name nil 536870912 true] ----  [2147483647 :name nil 2147483647 true]
  (d/q '[:find  ?n1 ?n2
         :where [?e1 :aka ?x]
         [?e2 :aka ?x]
         [?e1 :name ?n1]
         [?e2 :name ?n2]] @conn)
  )


 
(defn- get-range-as-byte-array
  "Returns fdb keys in the range [begin end] as a collection of byte-arrays. `begin` and `end` are vectors.
  index-type is `:eavt`, `:aevt` and `:avet`"
  [index-type begin end]
  (let [fd        (FDB/selectAPIVersion api-version)
        begin-key (KeySelector/firstGreaterOrEqual (key index-type begin))
        end-key   (KeySelector/firstGreaterThan (if (= (first end) 2147483647)
                                                  (max-key index-type)
                                                  (key index-type end))
                    #_(key index-type end) #_(max-key index-type)
                    )]
    (with-open [db (.open fd)]
      (tr! db 
        (mapv #(.getKey %)
          (.getRange tr begin-key end-key))))))

(defn get-range
  "Returns vectors in the range [begin end]. `begin` and `end` are vectors *in the [e a v t] form*. But it is really the index-type, i.e., `:eavt`, `:aevt` or `:avet` which sets the semantics of those vectors.
  Additionally, if nils are present in the `begin` vector they are replaced by :min-val to signal the system that we want the min. value at the spot. And conversely for `end` and :max-val."
  [index-type begin end]
  (let [replace-nil (fn [v new-val]
                      "replace nil in vector v by new-val"
                      (mapv #(if (nil? %) new-val %) v))
        new-begin (replace-nil begin :min-val)
        new-end (replace-nil end :max-val)
        res (get-range-as-byte-array index-type new-begin new-end)
        result (map (partial key->vect index-type) res)]
    ;; (println "Got from get-range: " (count res))
(println "*** In get-range: " index-type " -- " new-begin "----" new-end " --- RESULT: " result)
    result)) 

;;------------ KeySelectors and iterations

(defn get-key 
  "Returns the key behind a key-selector"
  [key-selector]
  (let [fd (FDB/selectAPIVersion api-version)]
    (with-open [db (.open fd)]
      (tr! db
           @(.getKey tr key-selector)))))

;; NOTE: Works but not used. Using range instead as it should be faster.
(defn iterate-from
  "Lazily iterates through the keys starting from `begin` (a key in fdb format)"
  [index-type begin]
  (let [key-selector (KeySelector/firstGreaterOrEqual (key index-type begin))
        key      (get-key key-selector)
        next-key (get-key (.add key-selector 1))]
    (when-not (= (seq key) (seq next-key)) ;; seq makes [B comparable
      (lazy-seq (cons key (iterate-from index-type next-key))))))


;;;;;;;;;;; Debug HELPER

;; ;; debug
;; (defn bArr
;;   [i]
;;   (let [arr (byte-array 1)]
;;     (aset-byte arr 0 i)
;;     arr))

;; (defn insert-int
;;   [db i]
;;   (let [fd    (FDB/selectAPIVersion api-version)
;;         key   (bArr i)
;;         ;; Putting the key also in the value
;;         value key]
;;     (with-open [db (.open fd)]
;;       (tr! db (.set tr key value))
;;       db)))


;; (defn get-range-int
;;   [db begin end]
;;   (let [fd        (FDB/selectAPIVersion api-version)
;;         begin-key (bArr begin)
;;         end-key   (bArr end)]
;;    (with-open [db (.open fd)]
;;      (tr! db ;;(.getRange tr (Range. (bArr 1) (bArr 2)))
;;           (.getRange tr begin-key end-key)
;;           ))))