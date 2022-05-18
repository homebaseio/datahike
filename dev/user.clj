(ns user
  (:require [datahike.api :as d]))

(comment

  (def schema [{:db/cardinality :db.cardinality/one
                :db/ident       :name
                :db/index       true
                :db/unique      :db.unique/identity
                :db/valueType   :db.type/string}
               {:db/ident       :parents
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/ref}
               {:db/ident       :age
                :db/cardinality :db.cardinality/one
                :db/valueType   :db.type/long}])

  (def cfg {:store              {:backend :mem
                                 :id "schema_upgrade"}
            :keep-history?      true
            :schema-flexibility :write
            :attribute-refs?    true})

  (do
    (d/delete-database cfg)
    (d/create-database cfg))

  (def conn (d/connect cfg))

  (d/transact conn {:tx-data schema})

  (d/schema @conn)

  (d/datoms @conn :eavt)

  (d/transact conn [{:name "Alice"
                     :age  25}
                    {:name "Bob"
                     :age  35}])

  (d/transact conn [{:name    "Charlie"
                     :age     5
                     :parents [[:name "Alice"] [:name "Bob"]]}])

  (d/q '[:find ?e ?n
         :where
         [?e :name ?n]] @conn)


  (d/transact conn {:tx-data [[:db/retractEntity [:db/ident :name]]]})

  (d/q '[:find ?e ?n
         :where
         [?e :name ?n]] @conn)

  )
