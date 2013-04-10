(defproject postgresql-tap "1.0.0"
  :description "Postgresql Tap for cascading"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.1" :exclusions [[cascalog/cascalog-elephantdb]
                                                 [cascalog/midje-cascalog]
                                                 [org.clojure/clojure]]]
                 [com.twitter/maple "0.2.4"]
                 [postgresql/postgresql "9.1-901.jdbc4"]
                 [org.apache.hadoop/hadoop-core "1.0.3"]]
  :java-source-paths ["src/"]
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :aot :all
  :jvm-opts ["-Xmx1g"])
