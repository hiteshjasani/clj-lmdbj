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

(deftest get-range-test
  (put! *db* "a" (s->bb! default-bb "1"))
  (put! *db* "b" (s->bb! default-bb "2"))
  (put! *db* "c" (s->bb! default-bb "3"))
  (put! *db* "d" (s->bb! default-bb "4"))
  (put! *db* "e" (s->bb! default-bb "5"))
  (testing "invalid range options throw exception"
    (is (thrown? IllegalArgumentException
                 (with-tx [tx (read-tx *env*)]
                   (get-range *db* tx :invalid-get-opt))))
    (is (thrown? IllegalArgumentException
                 (with-tx [tx (read-tx *env*)]
                   (get-range *db* tx [:invalid-get-opt])))))
  (testing "get all"
    (is (= [["a" "1"] ["b" "2"] ["c" "3"] ["d" "4"] ["e" "5"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx :all)))))
  (testing "get all reverse"
    (is (= [["e" "5"] ["d" "4"] ["c" "3"] ["b" "2"] ["a" "1"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx :all-reverse)))))
  (testing "at least 'c'"
    (is (= [["c" "3"] ["d" "4"] ["e" "5"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:at-least "c"])))))
  (testing "at least 'c' reversed"
    (is (= [["c" "3"] ["b" "2"] ["a" "1"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:at-least-reverse "c"])))))
  (testing "at most 'c'"
    (is (= [["a" "1"] ["b" "2"] ["c" "3"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:at-most "c"])))))
  (testing "at most 'c' reversed"
    (is (= [["e" "5"] ["d" "4"] ["c" "3"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:at-most-reverse "c"])))))
  (testing "closed 'b' to 'd'"
    (is (= [["b" "2"] ["c" "3"] ["d" "4"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:closed "b" "d"])))))
  (testing "closed 'd' to 'b' reversed"
    (is (= [["d" "4"] ["c" "3"] ["b" "2"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:closed-reverse "d" "b"])))))
  (testing "closed open 'b' to 'd'"
    (is (= [["b" "2"] ["c" "3"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:closed-open "b" "d"])))))
  (testing "closed open 'd' to 'b' reversed"
    (is (= [["d" "4"] ["c" "3"]]
           (with-tx [tx (read-tx *env*)]
             (get-range *db* tx [:closed-open-reverse "d" "b"])))))
  )

(deftest failing-tx-dont-persist-data
  (put! *db* "foo" (s->bb! default-bb "original"))
  (testing "failed tx won't persist"
    (is (= "original" (try
                        (with-tx [tx (write-tx *env*)]
                          (put! *db* tx "foo" (s->bb! default-bb "new"))
                          (throw (Exception. "fake exception")))
                        (catch Exception e
                          ;; ignore exception
                          (with-tx [tx (read-tx *env*)]
                            (get *db* tx "foo")))))))
  (testing "successful tx will persist"
    (is (= "new" (try
                   (with-tx [tx (write-tx *env*)]
                     (put! *db* tx "foo" (s->bb! default-bb "new")))
                   (with-tx [tx (read-tx *env*)]
                     (get *db* tx "foo"))
                   (catch Exception e
                     ;; ignore exception
                     (with-tx [tx (read-tx *env*)]
                       (get *db* tx "foo")))))))
  )
