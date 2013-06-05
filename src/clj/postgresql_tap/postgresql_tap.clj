(ns postgresql-tap.postgresql-tap
  (:use [cascalog.api])
  (:require [cascalog.ops :as c])
  (:import [com.twitter.maple.jdbc JDBCTap JDBCScheme TableDesc]
           [cascading.tuple Fields])
  (:gen-class))

(defn postgresql-tap
  [dbhost dbname dbusername dbpassword table primary-key columns]
  (let [dburl (str "jdbc:postgresql://" dbhost "/" dbname)
        fields (into-array String columns)
        primary (into-array String [primary-key])
        qmark (fn [input]
                (into-array String (map #(str "?" %) input)))
        jdbcscheme (JDBCScheme. (Fields. (qmark fields)) fields
                                primary
                                (Fields. (qmark primary)) primary)
        tabledesc (TableDesc. table (qmark fields) fields primary)
        tap (JDBCTap.
             dburl
             dbusername
             dbpassword
             "org.postgresql.Driver"
             tabledesc
             jdbcscheme)]
    tap))

;; (def tap (postgresql-tap "lily" "youmag" "youmag_ro" "passw" "website" "id" ["id" "cache_images" "uri"]))
