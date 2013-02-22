(ns postgresql-tap.postgresql-tap
  (:use [cascalog.api])
  (:require [cascalog.ops :as c]
            [clojure.string :as str])
  (:import [com.twitter.maple.jdbc JDBCTap JDBCScheme TableDesc])
  (:gen-class))

(defn postgresql-tap
  [dbhost dbname dbusername dbpassword table primary-key columns limit concurrent-reads]
  (let [dburl (str "jdbc:postgresql://" dbhost "/" dbname)

        tabledesc (com.twitter.maple.jdbc.TableDesc.
                   table                         ;; @param tableName of type String
                   (into-array columns)          ;; @param columnNames of type String[]
                   (into-array columns)          ;; @param columnDefs of type String[]
                   (into-array [ primary-key ])) ;; @param primaryKeys of type String

        jdbcscheme (com.twitter.maple.jdbc.JDBCScheme.
                    (into-array columns)                              ;; @param columns of type String[]
                    (str "SELECT " (str/join ", " columns)            ;; \
                         " FROM " table                               ;;  } @param selectQuery of type String
                         " ORDER BY " primary-key " DESC")            ;; /
                    (str "SELECT COUNT(" primary-key ") FROM " table) ;; @param countQuery of type String
                    limit)                                            ;; @param limit of type long

        tap (com.twitter.maple.jdbc.JDBCTap.
             dburl
             dbusername
             dbpassword
             "org.postgresql.Driver"
             tabledesc
             jdbcscheme)]
    (.setConcurrentReads tap concurrent-reads) ;; means 100 of limit 100 (limit / concurrent reads)
    tap))


;; (def tap (postgresql-tap "lily" "youmag" "youmag_ro" "devvedro" "website" "id" ["id" "cache_images" "uri"] 100 2))
;; (test-postgresql-tap tap (stdout))

(defn- test-postgresql-tap
  [input-tap output-tap]
  (?<- output-tap [?id ?other ?uri]
       (input-tap ?id ?other ?uri)))
