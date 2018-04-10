(ns clj-lmdbj.core
  (:refer-clojure :exclude [drop get])
  (:require [clojure.java.io :as io]
            [octet.core :as buf]
            )
  (:import (java.nio ByteBuffer)
           (org.lmdbjava DbiFlags Env EnvFlags KeyRange PutFlags)))

(def max-key-size 0)
(def key-bb nil)
(def default-bb-size 1024)
(def default-bb (buf/allocate default-bb-size {:type :direct :impl :nio}))

(defn s->bb!
  "Write string into ByteBuffer.  Returns the ByteBuffer."
  [^ByteBuffer bb s]
  (buf/write! bb s buf/string*)
  bb)

(defn bb->s
  "Read string from ByteBuffer"
  [^ByteBuffer bb]
  (buf/read bb (buf/string*)))

(defn to-kv
  [cursor-keyval]
  (let [k (bb->s (.key cursor-keyval))
        v (bb->s (.val cursor-keyval))]
    [k v]))

(defn allocate-key-buffer
  []
  (buf/allocate max-key-size {:type :direct :impl :nio}))

(defn create-env!
  "Create an environment.

  Warning: not thread safe!"
  [dirpath & {:keys [map-size max-dbs]
              :or {map-size 10485760 max-dbs 1}
              :as opts}]
  (let [env (-> (Env/create)
            (.setMapSize map-size)
            (.setMaxDbs max-dbs)
            (.open (io/file dirpath) (into-array EnvFlags [])))]
    ;; Dangerous, but we want something dynamic and fast
    (def max-key-size (.getMaxKeySize env))
    (def key-bb (allocate-key-buffer))
    env))

(defn create-db
  [env db-name]
  (.openDbi env db-name (into-array DbiFlags [DbiFlags/MDB_CREATE])))

(defn put!
  ([db k val-bb]
   (.put db (s->bb! key-bb k) val-bb))
  ([db tx k val-bb]
   (.put db tx (s->bb! key-bb k) val-bb (into-array PutFlags []))))

(defn get
  [db tx k]
  (bb->s (.get db tx (s->bb! key-bb k))))

(defn get-range
  "
  get-opt is one of:
    :all
    :all-reverse
    [:at-least <start-key>]
    [:at-least-reverse <start-key>]
    [:at-most <stop-key>]
    [:at-most-reverse <stop-key>]
    [:closed <start-key> <stop-key]
    [:closed-reverse <start-key> <stop-key>]
    [:closed-open <start-key> <stop-key>]
    [:closed-open-reverse <start-key> <stop-key>]
    [:> <start-key>]
    [:>-reverse <start-key>]
    [:< <stop-key>]
    [:<-reverse <stop-key>]
    [:open <start-key> <stop-key>]
    [:open-reverse <start-key> <stop-key>]
    [:open-closed <start-key> <stop-key>]
    [:open-closed-reverse <start-key> <stop-key>]
  "
  [db tx get-opt]
  (let [start-key (allocate-key-buffer)
        stop-key (allocate-key-buffer)
        keyrange-opt (if (keyword? get-opt)
                       (cond
                         (= :all get-opt) (KeyRange/all)
                         (= :all-reverse get-opt) (KeyRange/allBackward))
                       (condp = (first get-opt)
                         :at-least
                         (KeyRange/atLeast (s->bb! start-key (second get-opt)))
                         :at-least-reverse
                         (KeyRange/atLeastBackward
                          (s->bb! start-key (second get-opt)))
                         (throw (IllegalArgumentException.
                                 "Not a valid get-opt!"))))
        cursor (.iterate db tx keyrange-opt)]
    (loop [res []]
      (if (.hasNext cursor)
        (recur (conj res (to-kv (.next cursor))))
        res))))

(defn read-tx
  [env]
  (.txnRead env))

(defn write-tx
  [env]
  (.txnWrite env))

(defn drop
  "Delete all data in the database and keep it open"
  [db tx]
  (.drop db tx))

(defn close
  "Close the database handle.  Normally not needed - use with caution.
  Users should refer to the lmdbjava docs for more warnings."
  [db]
  (.close db))
