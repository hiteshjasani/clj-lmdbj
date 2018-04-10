(ns clj-lmdbj.core
  (:refer-clojure :exclude [drop get])
  (:require [clojure.java.io :as io]
            [octet.core :as buf]
            )
  (:import (java.nio ByteBuffer)
           (org.lmdbjava DbiFlags Env EnvFlags PutFlags)))

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
    (def key-bb (buf/allocate max-key-size {:type :direct :impl :nio}))
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
