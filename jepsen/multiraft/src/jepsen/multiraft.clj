(ns jepsen.multiraft
  "Local Jepsen suite for multiraft (CounterFsm via admin HTTP)."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [db :as db]
             [generator :as gen]
             [net :as net]
             [os :as os]
             [tests :as tests]]
            [jepsen.multiraft.client :as mr-client]
            [jepsen.multiraft.nemesis :as mr-nemesis]
            [jepsen.checker.timeline :as timeline])
  (:gen-class))

(defn env-int
  [k default]
  (if-let [v (System/getenv k)]
    (Integer/parseInt v)
    default))

(defn env-str
  [k default]
  (or (System/getenv k) default))

(defn workload
  "Add/read counter workload for checker/counter."
  [_opts]
  {:client    (mr-client/client)
   ;; Maps are one-shot generators; wrap in fns for an infinite stream.
   :generator (gen/mix [(fn [] {:type :invoke :f :read})
                        (fn [] {:type :invoke :f :add :value 1})])
   :checker   (checker/counter)})

(defn mr-test
  "Construct a local (dummy SSH) Jepsen test map.

  Cluster lifecycle is owned by scripts/run_jepsen.sh (JEPSEN=1 /
  NO_AUTO_PROPOSE=1); `:db` is noop."
  [opts]
  (let [w (workload opts)
        time-limit (or (:time-limit opts) (env-int "JEPSEN_TIME_LIMIT" 30))
        base-port (or (:base-port opts) (env-int "BASE_PORT" 23000))
        data-dir (or (:data-dir opts)
                     (env-str "DATA_DIR" (env-str "DATA" ".jepsen-data")))
        demo-bin (or (:demo-bin opts)
                     (env-str "DEMO_BIN"
                              (str (env-str "MULTIRAFT_ROOT" ".")
                                   "/target/debug/multiraft-demo")))
        groups (or (:groups opts) (env-int "GROUPS" 1))
        ;; Prefer CLI --nodes 1,2,3; fall back if defaults are n1..n5.
        nodes (let [ns (:nodes opts)]
                (if (and (seq ns) (every? #(re-matches #"\d+" (str %)) ns))
                  (mapv str ns)
                  ["1" "2" "3"]))]
    (merge tests/noop-test
           opts
           {:name            "multiraft"
            :os              os/noop
            :db              db/noop
            :net             net/noop
            :ssh             {:dummy? true}
            :nodes           nodes
            :base-port       base-port
            :data-dir        data-dir
            :demo-bin        demo-bin
            :groups          groups
            :node-count      (count nodes)
            :client          (:client w)
            :nemesis         (mr-nemesis/process-killer)
            :generator       (->> (:generator w)
                                  (gen/stagger 1/50)
                                  (gen/nemesis
                                   (gen/cycle
                                    [(gen/sleep 4)
                                     {:type :info :f :kill}
                                     (gen/sleep 4)
                                     {:type :info :f :start}]))
                                  (gen/time-limit time-limit))
            :checker         (checker/compose
                              {:counter  (:checker w)
                               :timeline (timeline/html)
                               :stats    (checker/stats)})
            :pure-generators true})))

(defn -main
  [& args]
  (info "multiraft Jepsen starting" args)
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn mr-test})
          (cli/serve-cmd))
   args))
