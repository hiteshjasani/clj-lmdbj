# clj-lmdbj

A Clojure library interface to [lmdbjava](https://github.com/lmdbjava/lmdbjava).

## Usage

```clojure
(require '[clj-lmdbj.core :as l])

(def env (l/create-env! "/tmp"))
(def db (l/create-db env "mydb"))

# Add keys
(l/put! db "foo" (l/s->bb! default-bb "baz"))


```

## License

Copyright © 2018 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
