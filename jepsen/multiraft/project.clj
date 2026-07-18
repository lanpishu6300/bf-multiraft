(defproject jepsen.multiraft "0.1.0-SNAPSHOT"
  :description "Local (no SSH VMs) Jepsen suite for multiraft CounterFsm"
  :url "https://github.com//multiraft"
  :license {:name "Proprietary"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [jepsen "0.3.9"]
                 [clj-http "3.13.0"]
                 [cheshire "5.13.0"]]
  :main jepsen.multiraft
  :jvm-opts ["-Xmx2g"
             "-Djdk.attach.allowAttachSelf=true"]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}})
