(ns clj-lmdbj.core
  (:refer-clojure :exclude [drop get])
  (:require [clojure.java.io :as io]
            [octet.core :as buf]
            )
  (:import (java.nio ByteBuffer)
           (org.lmdbjava CursorIterator CursorIterator$KeyVal Dbi
                         DbiFlags Env EnvFlags KeyRange PutFlags
                         Txn)))

(defn allocate-buffer
  "Allocate a direct ByteBuffer of size"
  [size]
  (buf/allocate size {:type :direct :impl :nio}))

(def max-key-size 0)
(def key-bb nil)
(def default-bb-size 1024)
(def default-bb (allocate-buffer default-bb-size))

(defn s->bb!
  "Write string into ByteBuffer.  Returns the modified ByteBuffer.

  The arity 1 version writes to a shared default ByteBuffer that has a
  max size of 1024 bytes.  If you want to write more than that you
  should allocate a larger ByteBuffer and use the arity 2 version."
  ([^String s] (s->bb! default-bb s))
  ([^ByteBuffer bb ^String s]
   (buf/write! bb s buf/string*)
   bb))

(defn bb->s
  "Read string from ByteBuffer"
  [^ByteBuffer bb]
  (buf/read bb (buf/string*)))

(defn to-kv
  "Extract key and value from a cursor keyval"
  [^CursorIterator$KeyVal cursor-keyval]
  (let [k (bb->s (.key cursor-keyval))
        v (bb->s (.val cursor-keyval))]
    [k v]))

(defn ^Env create-env!
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
    (def key-bb (allocate-buffer max-key-size))
    env))

(defn ^Dbi create-db
  [^Env env ^String db-name]
  (.openDbi env db-name (into-array DbiFlags [DbiFlags/MDB_CREATE])))

(defn put!
  ([^Dbi db k val-bb]
   (.put db (s->bb! key-bb k) val-bb))
  ([^Dbi db tx k val-bb]
   (.put db tx (s->bb! key-bb k) val-bb (into-array PutFlags []))))

(defn get
  [^Dbi db tx k]
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
  [^Dbi db ^Txn tx get-opt]
  (let [start-key (allocate-buffer max-key-size)
        stop-key (allocate-buffer max-key-size)
        ^KeyRange keyrange-opt
        (if (keyword? get-opt)
          (cond
            (= :all get-opt) (KeyRange/all)
            (= :all-reverse get-opt) (KeyRange/allBackward)
            :else (throw (IllegalArgumentException.
                          "Not a valid get-opt!")))
          (condp = (first get-opt)
            :at-least (KeyRange/atLeast (s->bb! start-key (second get-opt)))
            :at-least-reverse (KeyRange/atLeastBackward
                               (s->bb! start-key (second get-opt)))
            :at-most (KeyRange/atMost (s->bb! stop-key (second get-opt)))
            :at-most-reverse (KeyRange/atMostBackward
                              (s->bb! stop-key (second get-opt)))
            :closed (KeyRange/closed (s->bb! start-key (nth get-opt 1))
                                     (s->bb! stop-key (nth get-opt 2)))
            :closed-reverse (KeyRange/closedBackward
                             (s->bb! start-key (nth get-opt 1))
                             (s->bb! stop-key (nth get-opt 2)))
            :closed-open (KeyRange/closedOpen (s->bb! start-key (nth get-opt 1))
                                              (s->bb! stop-key (nth get-opt 2)))
            :closed-open-reverse (KeyRange/closedOpenBackward
                                  (s->bb! start-key (nth get-opt 1))
                                  (s->bb! stop-key (nth get-opt 2)))
            :> (KeyRange/greaterThan (s->bb! start-key (nth get-opt 1)))
            :>-reverse (KeyRange/greaterThanBackward
                        (s->bb! start-key (nth get-opt 1)))
            :< (KeyRange/lessThan (s->bb! stop-key (nth get-opt 1)))
            :<-reverse (KeyRange/lessThanBackward
                        (s->bb! stop-key (nth get-opt 1)))
            :open (KeyRange/open (s->bb! start-key (nth get-opt 1))
                                 (s->bb! stop-key (nth get-opt 2)))
            :open-reverse (KeyRange/openBackward
                           (s->bb! start-key (nth get-opt 1))
                           (s->bb! stop-key (nth get-opt 2)))
            :open-closed (KeyRange/openClosed (s->bb! start-key (nth get-opt 1))
                                              (s->bb! stop-key (nth get-opt 2)))
            :open-closed-reverse (KeyRange/openClosedBackward
                                  (s->bb! start-key (nth get-opt 1))
                                  (s->bb! stop-key (nth get-opt 2)))
            (throw (IllegalArgumentException.
                    "Not a valid get-opt!"))))
        ^CursorIterator cursor (.iterate db tx keyrange-opt)]
    (loop [res []]
      (if (.hasNext cursor)
        (recur (conj res (to-kv (.next cursor))))
        res))))

(defn ^Txn read-tx
  [^Env env]
  (.txnRead env))

(defn ^Txn write-tx
  [^Env env]
  (.txnWrite env))

(defn drop
  "Delete all data in the database and keep it open"
  [^Dbi db tx]
  (.drop db tx))

(defn close
  "Close the database handle.  Normally not needed - use with caution.
  Users should refer to the lmdbjava docs for more warnings."
  [^Dbi db]
  (.close db))
