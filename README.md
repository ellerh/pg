
## pg.el ##

This module lets you access the PostgreSQL object-relational DBMS from
Emacs, using its socket-level [frontend/backend
protocol](https://www.postgresql.org/docs/current/protocol.html). The
module is capable of automatic type coercions from a range of SQL
types to the equivalent Emacs Lisp type. This is a low level API, and
won't be useful to end users. 

Should work with Emacs version 24 or newer and Postgres 9 or
newer. The module only supports version 3 of the protocol with md5 or
scram-sha-256 based [password
authentication](https://www.postgresql.org/docs/current/auth-password.html).
