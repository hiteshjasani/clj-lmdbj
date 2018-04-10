(ns clj-lmdbj.core-test
  (:refer-clojure :exclude [drop get])
  (:require [clojure.test :refer :all]
            [clj-lmdbj.core :refer :all]
            [clj-lmdbj.macros :refer :all]))

(def ^:dynamic *env* nil)
(def ^:dynamic *db* nil)

(defn with-db-fixture [f]
  (binding [*env* (create-env! "/tmp")]
    (binding [*db* (create-db *env* "clj-lmdbj-core-test")]
      ;; (println "with-db-fixture/pre")
      (f)
      ;; teardown code
      ;; (println "with-db-fixture/post")
      )))

(defn with-empty-db-fixture [f]
  ;; (println "with-empty-db-fixture/pre")
  (f)
  ;; (println "with-empty-db-fixture/post")
  (with-tx [tx (write-tx *env*)]
    (drop *db* tx)))

(use-fixtures :once with-db-fixture)
(use-fixtures :each with-empty-db-fixture)

(deftest vars-test
  (testing "key bytebuffer"
    #_(is (zero? max-key-size))
    #_(is (nil? key-bb))
    #_(is (not (nil? (create-env! "/tmp"))))
    (is (= 511 max-key-size))
    (is (not (nil? key-bb)))
    (is (= 1024 default-bb-size))
    (is (not (nil? default-bb)))
    ))

(deftest simple-kv-test
  (testing "single key read and write"
    (is (= "bar" (do (put! *db* "foo" (s->bb! default-bb "bar"))
                     (with-tx [tx (read-tx *env*)]
                       (get *db* tx "foo"))))))
  (testing "multiple keys reading and writing"
    (put! *db* "foo" (s->bb! default-bb "baz"))
    (put! *db* "bar" (s->bb! default-bb "quux"))
    (is (= ["baz" "quux"] (with-tx [tx (read-tx *env*)]
                            [(get *db* tx "foo") (get *db* tx "bar")]))))
  )
