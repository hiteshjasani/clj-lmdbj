# clj-lmdbj

A Clojure library interface to [lmdbjava](https://github.com/lmdbjava/lmdbjava).

## Usage

```clojure
;; avoid conflicting with clojure.core's `get` and `drop`
(refer 'clj-lmdbj.core :exclude '[get drop]])
(require '[clj-lmdbj.core :as l])
(require '[clj-lmdbj.macros :refer :all])

(def env (create-env! "/tmp"))
(def db (create-db env "mydb"))

;; Add keys
(put! db "foo" (s->bb! "bar"))
(put! db "baz" (s->bb! "quux"))

;; Get keys, must always be in a transaction
(with-tx [tx (read-tx env)]
  (println (str "foo = " (l/get db tx "foo")))
  (println (str "baz = " (l/get db tx "baz"))))

;; Delete all data
(with-tx [tx (write-tx env)]
  (l/drop db tx))

;; Add some keys in a single transaction
(with-tx [tx (write-tx env)]
  (put! db tx "a" (s->bb! "1"))
  (put! db tx "b" (s->bb! "2"))
  (put! db tx "c" (s->bb! "3"))
  (put! db tx "d" (s->bb! "4"))
  (put! db tx "e" (s->bb! "5")))

;; Do some range queries

;; get all
(with-tx [tx (read-tx env)]
  (get-range db tx :all))
;;=> [["a" "1"] ["b" "2"] ["c" "3"] ["d" "4"] ["e" "5"]]

;; get all reversed
(with-tx [tx (read-tx env)]
  (get-range db tx :all-reverse))
;;=> [["e" "5"] ["d" "4"] ["c" "3"] ["b" "2"] ["a" "1"]]

;; get range starting with "c"
(with-tx [tx (read-tx env)]
  (get-range db tx [:at-least "c"])))))
;;=> [["c" "3"] ["d" "4"] ["e" "5"]]

;; get range up to "c"
(with-tx [tx (read-tx env)]
  (get-range db tx [:at-most "c"])))))
;;=> [["a" "1"] ["b" "2"] ["c" "3"]]

;; get range from "b" to "d" inclusive
(with-tx [tx (read-tx env)]
  (get-range db tx [:closed "b" "d"])))))
[["b" "2"] ["c" "3"] ["d" "4"]]
```

Refer to the tests to see more examples of range queries.

## License

Copyright © 2018 Hitesh Jasani

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
