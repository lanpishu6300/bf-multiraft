(ns jepsen.multiraft-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.multiraft.client :as client]))

(deftest admin-url-mapping
  (let [test {:base-port 23000}]
    (is (= "http://127.0.0.1:23100" (client/admin-url test "1")))
    (is (= "http://127.0.0.1:23101" (client/admin-url test "2")))
    (is (= "http://127.0.0.1:23102" (client/admin-url test 3)))))
