(ns es-cp.core
  (:require [clojurewerkz.elastisch.rest.document      :as doc]
            [clojurewerkz.elastisch.rest.index         :as idx]
            [clojurewerkz.elastisch.rest               :as esr]
            [clojurewerkz.elastisch.query         :as q]

            [cheshire.core :as json]
            [clj-http.client :as http])
  (:use clojure.test
        [clojurewerkz.elastisch.rest.response :only [ok? acknowledged? conflict? hits-from any-hits? no-hits?]]
        [clojure.string :only [join]]))

(defn index-operation
  [doc]
  {"index"
   (select-keys doc [:_index :_type :_id])})

(defn bulk-index
  "generates the content for a bulk insert operation"
  ([documents]
     (let [operations (map index-operation documents)]
       (interleave operations documents))))

(def index-name "people")
(def index-type "person")
(def new-index  "people3")

(defn copy-results
  [scroll-id]
  (let [scroll-response (doc/scroll scroll-id :scroll "1m")
        hits            (hits-from scroll-response)]
    (if (seq hits)
      (do
        ;; index
        (println (doc/bulk-index new-index (bulk-index (map #( dissoc % :_index) hits)) ))
        (recur (:_scroll_id scroll-response)))
      true)))

(defn copy
  [source-url dest-url query]
  (let [ results (doc/search index-name index-type
                            :query (q/query-string :query query)
                            :search_type "scan"
                            :scroll "1m"
                            :size 100)
        hits (hits-from results)]


    (copy-results (:_scroll_id results))))
