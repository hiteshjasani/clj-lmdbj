(ns clj-lmdbj.core-test
  (:refer-clojure :exclude [get])
  (:require [clojure.test :refer :all]
            [clj-lmdbj.core :refer :all]
            [clj-lmdbj.macros :refer :all]))

(deftest vars-test
  (testing "key bytebuffer"
    #_(is (zero? max-key-size))
    #_(is (nil? key-bb))
    (is (not (nil? (create-env! "/tmp"))))
    (is (not (nil? key-bb)))
    (is (= 511 max-key-size))))

(deftest simple-kv-test
  (let [env (create-env! "/tmp")
        db (create-db env "simple-kv-test")]
    (testing "single key read and write"
      (is (= "bar" (do (put! db "foo" (s->bb! default-bb "bar"))
                       (with-tx [tx (read-tx env)]
                         (get db tx "foo"))))))))
