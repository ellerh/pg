;;; pg.el --- Interface to PostgreSQL             -*- lexical-binding: t -*-
;;;
;;; Author: Eric Marsden <emarsden@laas.fr>
;;; Maintainer: Helmut Eller <heller@common-lisp.net>
;;;
;;; Version: 0.13
;;; Keywords: data comm database postgresql
;;; Copyright: (C) 1999-2005  Eric Marsden
;;
;;     This program is free software; you can redistribute it and/or
;;     modify it under the terms of the GNU General Public License as
;;     published by the Free Software Foundation; either version 2 of
;;     the License, or (at your option) any later version.
;;
;;     This program is distributed in the hope that it will be useful,
;;     but WITHOUT ANY WARRANTY; without even the implied warranty of
;;     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
;;     GNU General Public License for more details.
;;
;;     You should have received a copy of the GNU General Public
;;     License along with this program; if not, write to the Free
;;     Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
;;     MA 02111-1307, USA.
;;


;;; Commentary:

;;; Overview ==========================================================
;;
;; This module lets you access the PostgreSQL object-relational DBMS
;; from Emacs, using its socket-level frontend/backend protocol. The
;; module is capable of automatic type coercions from a range of SQL
;; types to the equivalent Emacs Lisp type. This is a low level API,
;; and won't be useful to end users. Should work with Emacs version 24
;; or newer and Postgres versions that support version 3 of the
;; protocol (tested with Postgres version 9 and 14).

;;; Entry points =======================================================
;;
;; The function names don't follow the usual Emacs conventions but we
;; keep them for backward compatibility.
;;
;; (with-pg-connection con (dbname user [password host port]) &body body)
;;     A macro which opens a connection to database DBNAME, executes the
;;     BODY forms then disconnects. See function `pg:connect' for details
;;     of the connection arguments.
;;
;; (with-pg-transaction con &body body)
;;     A macro which executes the BODY forms wrapped in an SQL transaction.
;;     CON is a connection to the database. If an error occurs during the
;;     execution of the forms, a ROLLBACK instruction is executed.
;;
;; (pg:connect dbname user [password host port]) -> connection
;;     Connect to the database DBNAME on HOST (defaults to localhost)
;;     at PORT (defaults to 5432) via TCP/IP and log in as USER. If
;;     the database requires a password, send PASSWORD as clear text.
;;     Set the output date type to 'ISO', and initialize our type
;;     parser tables.
;;
;; (pg:exec connection &rest sql) -> pgresult
;;     Concatenate the SQL strings and send to the backend. Retrieve
;;     all the information returned by the database and return it in
;;     an opaque record PGRESULT.
;;
;; (pg:result pgresult what &rest args) -> info
;;     Extract information from the PGRESULT. The WHAT keyword can be
;;     one of
;;          * :connection
;;          * :status
;;          * :attributes
;;          * :tuples
;;          * :tuple tupleNumber
;;          * :oid
;;     `:connection' allows you to retrieve the database connection.
;;     `:status' is a string returned by the backend to indicate the
;;     status of the command; it is something like "SELECT" for a
;;     select command, "DELETE 1" if the deletion affected a single
;;     row, etc. `:attributes' is a list of tuples providing metadata:
;;     the first component of each tuple is the attribute's name as a
;;     string, the second an integer representing its PostgreSQL type,
;;     and the third an integer representing the size of that type.
;;     `:tuples' returns all the data retrieved from the database, as a
;;     list of lists, each list corresponding to one row of data
;;     returned by the backend. `:tuple num' can be used to extract a
;;     specific tuple (numbering starts at 0). `:oid' allows you to
;;     retrieve the OID returned by the backend if the command was an
;;     insertion; the OID is a unique identifier for that row in the
;;     database (this is PostgreSQL-specific, please refer to the
;;     documentation for more details).
;;
;; (pg:disconnect connection) -> nil
;;     Close the database connection.
;;
;; (pg:for-each connection select-form callback)
;;     Calls CALLBACK on each tuple returned by SELECT-FORM. Declares
;;     a cursor for SELECT-FORM, then fetches tuples using repeated
;;     executions of FETCH 1, until no results are left. The cursor is
;;     then closed. The work is performed within a transaction. When
;;     you have a large amount of data to handle, this usage is more
;;     efficient than fetching all the tuples in one go.
;;
;;     If you wish to browse the results, each one in a separate
;;     buffer, you could have the callback insert each tuple into a
;;     buffer created with (generate-new-buffer "myprefix"), then use
;;     ibuffer's "/ n" to list/visit/delete all buffers whose names
;;     match myprefix.
;;
;; (pg:databases connection) -> list of strings
;;     Return a list of the databases available at this site (a
;;     database is a set of tables; in a virgin PostgreSQL
;;     installation there is a single database named "template1").
;;
;; (pg:tables connection) -> list of strings
;;     Return a list of the tables present in the database to which we
;;     are currently connected. Only include user tables: system
;;     tables are excluded.
;;
;; (pg:columns connection table) -> list of strings
;;     Return a list of the columns (or attributes) in TABLE, which
;;     must be a table in the database to which we are currently
;;     connected. We only include the column names; if you want more
;;     detailed information (attribute types, for example), it can be
;;     obtained from `pg:result' on a SELECT statement for that table.
;;
;; (pg:lo-create conn . args) -> oid
;;     Create a new large object (BLOB, or binary large object in
;;     other DBMSes parlance) in the database to which we are
;;     connected via CONN. Returns an OID (which is represented as an
;;     elisp integer) which will allow you to use the large object.
;;     Optional ARGS are a Unix-style mode string which determines the
;;     permissions of the newly created large object, one of "r" for
;;     read-only permission, "w" for write-only, "rw" for read+write.
;;     Default is "r".
;;
;;     Large-object functions MUST be used within a transaction (see
;;     the macro `with-pg-transaction').
;;
;; (pg:lo-open conn oid . args) -> fd
;;     Open a large object whose unique identifier is OID (an elisp
;;     integer) in the database to which we are connected via CONN.
;;     Optional ARGS is a Unix-style mode string as for pg:lo-create;
;;     which defaults to "r" read-only permissions. Returns a file
;;     descriptor (an elisp integer) which can be used in other
;;     large-object functions.
;;
;; (pg:lo-close conn fd)
;;     Close the file descriptor FD which was associated with a large
;;     object. Note that this does not delete the large object; use
;;     `pg:lo-unlink' for that.
;;
;; (pg:lo-read conn fd bytes) -> string
;;     Read BYTES from the file descriptor FD which is associated with
;;     a large object. Return an elisp string which should be BYTES
;;     characters long.
;;
;; (pg:lo-write connection fd buf)
;;     Write the bytes contained in the elisp string BUF to the
;;     large object associated with the file descriptor FD. 
;;
;; (pg:lo-lseek conn fd offset whence)
;;     Do the equivalent of a lseek(2) on the file descriptor FD which
;;     is associated with a large object; ie reposition the read/write
;;     file offset for that large object to OFFSET (an elisp
;;     integer). WHENCE has the same significance as in lseek(); it
;;     should be one of SEEK_SET (set the offset to the absolute
;;     position), SEEK_CUR (set the offset relative to the current
;;     offset) or SEEK_END (set the offset relative to the end of the
;;     file). WHENCE should be an elisp integer whose values can be
;;     obtained from the header file <unistd.h> (probably 0, 1 and 2
;;     respectively).
;;
;; (pg:lo-tell conn oid) -> integer
;;     Do the equivalent of an ftell(3) on the file associated with
;;     the large object whose unique identifier is OID. Returns the
;;     current position of the file offset for the object's associated
;;     file descriptor, as an elisp integer.
;;
;; (pg:lo-unlink conn oid)
;;     Remove the large object whose unique identifier is OID from the
;;     system (in the current implementation of large objects in
;;     PostgreSQL, each large object is associated with an object in
;;     the filesystem).
;;
;; (pg:lo-import conn filename) -> oid
;;     Create a new large object and initialize it to the data
;;     contained in the file whose name is FILENAME. Returns an OID
;;     (as an elisp integer). Note that is operation is only syntactic
;;     sugar around the basic large-object operations listed above.
;;
;; (pg:lo-export conn oid filename)
;;     Create a new file named FILENAME and fill it with the contents
;;     of the large object whose unique identifier is OID. This
;;     operation is also syntactic sugar.
;;
;;
;; Boolean variable `pg:disable-type-coercion' which can be set to
;; non-nil (before initiating a connection) to disable the library's
;; type coercion facility. Default is t.
;;
;;
;; The interface is pretty slow (byte compiling helps a lot). Maybe
;; someone can suggest a better way of reading input from the network
;; stream. Please note that your postmaster has to be started with the
;; `-i' option in order to accept TCP/IP connections (this is not the
;; default). For more information about PostgreSQL see
;; <URL:http://www.PostgreSQL.org/>.
;;
;; Thanks to Eric Ludlam <zappo@gnu.org> for discovering a bug in the
;; date parsing routines, to Hartmut Pilch and Yoshio Katayama for
;; adding multibyte support, and to Doug McNaught and Pavel Janik for
;; bug fixes.


;; SECURITY NOTE: setting up PostgreSQL to accept TCP/IP connections
;; has security implications; please consult the documentation for
;; details. pg.el supports neither the crypt authentication method,
;; nor Kerberos (support for these can't be added to Emacs due to
;; silly US crypto export regulations). However, it is possible to use
;; the port forwarding capabilities of ssh to establish a connection
;; to the backend over TCP/IP, which provides both a secure
;; authentication mechanism and encryption (and optionally
;; compression) of data passing through the tunnel. Here's how to do
;; it (thanks to Gene Selkov, Jr. <selkovjr@mcs.anl.gov> for the
;; description):
;;
;; 1. Establish a tunnel to the backend machine, like this:
;;
;;	ssh -L 3333:backend.dom:5432 postgres@backend.dom
;;
;;    The first number in the -L argument, 3333, is the port number of
;;    your end of the tunnel. The second number, 5432, is the remote
;;    end of the tunnel -- the port number your backend is using. The
;;    name or the address in between the port numbers belongs to the
;;    server machine, as does the last argument to ssh that also includes
;;    the optional user name. Without the user name, ssh will try the
;;    name you are currently logged on as on the client machine. You can
;;    use any user name the server machine will accept, not necessarily
;;    those related to postgres.
;;
;; 2. Now that you have a running ssh session, you can point pg.el to
;;    the local host at the port number which you specified in step 1.
;;    For example,
;;
;;         (pg:connect "dbname" "user" "password" "localhost" 3333)
;;
;;    You can omit the port argument if you chose 5432 as the local
;;    end of the tunnel, since pg.el defaults to this value.


;;; INSTALL =========================================================
;;
;; Place this file in a directory somewhere in the load-path, then
;; byte-compile it (do a `B' on it in dired, for example). Place a
;; line such as `(require 'pg)' in your emacs initialization file.


;;; TODO ============================================================
;;
;; * add a mechanism for parsing user-defined types. The user should
;;   be able to define a parse function and a type-name; we query
;;   pg_type to get the type's OID and add the information to
;;   pg--parsers.
;;
;; * in a future release I will probably modify the numeric conversion
;;   routines to return elisp floating point values instead of elisp
;;   integers, in order to work around possible overflow problems.


;;; Code:

(require 'cl-lib)
(require 'sasl)

(cl-defstruct (pg--conn (:constructor pg--make-conn)
			(:predicate pg--conn?) (:copier nil))
  proc sndbuf params pid secret)

(cl-defstruct (pg--result (:constructor pg--make-result)
			  (:predicate pg--result?) (:copier nil))
  conn status attrs tuples portal)

(cl-defstruct (pg--attrdesc (:constructor pg--make-attrdesc)
			    (:predicate pg--attrdesc?) (:copier nil))
  name tableid columnid typeid typelen atttypmod format)

(defun pg--get-char ()
  (prog1 (char-after (point-min))
    (delete-region (point-min) (1+ (point-min)))))

;; read a big-endian signed integer
(defun pg--get-int (bytes)
  (cl-ecase bytes
    (4 (let* ((b0 (pg--get-char))
	      (b1 (pg--get-char))
	      (b2 (pg--get-char))
	      (b3 (pg--get-char)))
	 (logior (if (= (logand b0 #x80) 0) 0 (ash -1 32))
		 (ash b0 24) (ash b1 16) (ash b2 8) b3)))
    (2 (let* ((b0 (pg--get-char))
	      (b1 (pg--get-char)))
	 (logior (if (= (logand b0 #x80) 0) 0 (ash -1 16))
		 (ash b0 8) b1)))))

(defun pg--get-bytes (len)
  (prog1 (buffer-substring-no-properties (point-min) (+ (point-min) len))
    (delete-region (point-min) (+ (point-min) len))))

(defun pg--get-utf8 ()
  (goto-char (point-min))
  (skip-chars-forward "^\0")
  (cl-assert (= (char-after) 0))
  (prog1 (decode-coding-region (point-min) (point) 'utf-8 t)
    (delete-region (point-min) (1+ (point)))))

(defun pg--read-pkt (connection fun)
  (let ((proc (pg--conn-proc connection)))
    (with-current-buffer (process-buffer proc)
      (delete-region (point-min) (point-max))
      (widen)
      (while (< (buffer-size) 5)
	(accept-process-output proc 0.2))
      (let ((type (pg--get-char))
	    (len (pg--get-int 4)))
	(while (< (buffer-size) (- len 4))
	  (accept-process-output proc 0.2))
	(narrow-to-region (point-min) (+ (point-min) (- len 4)))
	(funcall fun type len)))))

(defun pg--process-filter (proc str)
  ;; (let ((print-escape-control-characters t))
  ;;   (message "filter: %S" str))
  (with-current-buffer (process-buffer proc)
    (save-restriction
      (widen)
      (save-excursion
	(goto-char (point-max))
	(insert str)))))

(defvar pg:disable-type-coercion nil
  "*Non-nil disables the type coercion mechanism.
The default is nil, which means that data recovered from the database
is coerced to the corresponding Emacs Lisp type before being returned;
for example numeric data is transformed to Emacs Lisp numbers, and
booleans to booleans.

The coercion mechanism requires an initialization query to the
database, in order to build a table mapping type names to OIDs. This
option is provided mainly in case you wish to avoid the overhead of
this initial query. The overhead is only incurred once per Emacs
session (not per connection to the backend).")

;; alist of (oid . parser) pairs. This is built dynamically at
;; initialization of the connection with the database (once generated,
;; the information is shared between connections).
(defvar pg--parsers '())

(defconst pg:AUTH_REQ_OK       0)
(defconst pg:AUTH_REQ_KRB4     1)
(defconst pg:AUTH_REQ_KRB5     2)
(defconst pg:AUTH_REQ_PASSWORD 3)
(defconst pg:AUTH_REQ_CRYPT    4)
(defconst pg:AUTH_REQ_MD5      5)
(defconst pg:AUTH_REQ_SASL     10)
(defconst pg:AUTH_REQ_SASL_CONT 11)
(defconst pg:AUTH_REQ_SASL_FIN  12)

(defconst pg:MAX_MESSAGE_LEN    8192)   ; libpq-fe.h

;; big-endian
(defun pg--insert-int (num bytes)
  (let ((i bytes))
    (while (> i 0)
      (setq i (1- i))
      (insert (logand (ash num (* i -8)) 255)))))

(defun pg--insert-utf8 (string)
  (forward-char (encode-coding-string string 'utf-8 t (current-buffer))))

(defun pg--insert-string (string)
  (pg--insert-utf8 string)
  (insert 0))

(defun pg--insert-bytes (bytes)
  (insert bytes))

(defun pg--insert-startup-packet (dbname user)
  (pg--insert-int 3 2) ; major protocol version
  (pg--insert-int 0 2) ; minor protocol version
  (let ((f (lambda (key value)
	     (pg--insert-string key)
	     (pg--insert-string value))))
    (funcall f "database" dbname)
    (funcall f "user" user)
    (funcall f "client_encoding" "UTF-8")
    (funcall f "datestyle" "ISO"))
  (pg--insert-string ""))

(defun pg--send-pkt (connection type fun)
  (with-current-buffer (pg--conn-sndbuf connection)
    (when type
      (pg--insert-int type 1))
    (pg--insert-int 0 4) ;; room for length field
    (funcall fun)
    ;; overwrite length field
    (let ((len (buffer-size))
	  (pos (point-min)))
      (when type
	(setq len (1- len))
	(setq pos (1+ pos)))
      (goto-char pos)
      (delete-region pos (+ pos 4))
      (pg--insert-int len 4))
    ;; (let ((print-escape-control-characters t))
    ;;   (message "send-pkt: %S\n" (buffer-string)))
    (process-send-region (pg--conn-proc connection) (point-min) (point-max))
    (erase-buffer)))

(defun pg--read-connection-params (connection)
  (let ((f (lambda (type _len)
	     (cl-ecase type
	       (?S (let ((key (pg--get-utf8))
			 (val (pg--get-utf8)))
		     (push (cons key val) (pg--conn-params connection))
		     nil))
	       (?K (let ((pid (pg--get-int 4))
			 (key (pg--get-int 4)))
		     (setf (pg--conn-pid connection) pid)
		     (setf (pg--conn-secret connection) key)
		     nil))
	       (?Z (let ((c (pg--get-char)))
		     (cl-ecase c
		       (?I t))))))))
    (while (not (pg--read-pkt connection f)))))

(defun pg--get-error-notice ()
  (let ((result '()))
    (while (let ((c (pg--get-char)))
	     (cond ((= c 0) nil)
		   (t
		    (push (cons c (pg--get-utf8)) result)
		    t))))
    (reverse result)))

(defun pg--make-unibyte-buffer (name)
  (let ((buf (generate-new-buffer name)))
    (with-current-buffer buf
      (set-buffer-multibyte nil))
    buf))

(defun pg--make-connection (proc-fun)
  (let* ((buf (pg--make-unibyte-buffer " *PostgreSQL*"))
	 (sndbuf (pg--make-unibyte-buffer " *PostgreSQL sndbuf*"))
	 (proc (funcall proc-fun buf)))
    (set-process-coding-system proc 'binary 'binary)
    (set-process-filter proc #'pg--process-filter)
    (pg--make-conn :proc proc :sndbuf sndbuf)))

(define-error 'pg-error "Postgres error")

(defun pg--signal-error (_connection args) (signal 'pg-error args))

(defun pg--insert-sasl-step (step)
  (pg--insert-bytes (sasl-step-data step)))

(defun pg--insert-first-sasl-client-message (client step)
  (let* ((name (sasl-mechanism-name (sasl-client-mechanism client))))
    (pg--insert-string name)
    (pg--insert-int (length (sasl-step-data step)) 4)
    (pg--insert-sasl-step step)))

(defun pg--sasl-continue (client step type len)
  (cl-ecase type
    (?R
     (let ((req (pg--get-int 4)))
       (cond ((= req pg:AUTH_REQ_SASL_CONT)
	      (let* ((challenge (pg--get-bytes (- len 8))))
		(sasl-step-set-data step challenge)))
	     (
	      (error "Expected AUTH_REQ_SASL_CONT: %d" req)))))))

(defun pg--sasl-final (client step type len)
  (cl-ecase type
    (?R
     (let ((req (pg--get-int 4)))
       (cond ((= req pg:AUTH_REQ_SASL_FIN)
	      (let* ((challenge (pg--get-bytes (- len 8))))
		(sasl-step-set-data step challenge)))
	     (
	      (error "Expected AUTH_REQ_SASL_FIN: %d" req)))))))

(defun pg--read-sasl-requests (connection user password)
  (let* ((mechanisms '())
	 (_ (while (let ((s (pg--get-utf8)))
		     (cond ((equal s "") nil)
			   (t (push s mechanisms))))))
	 (mechanism (or (sasl-find-mechanism mechanisms)
			(error "No supported SASL mechanism: %S" mechanisms)))
	 (client (sasl-make-client mechanism user "pg.el" "postgres"))
	 (step (sasl-next-step client nil)))
    (pg--send-pkt connection ?p
		  (lambda ()
		    (pg--insert-first-sasl-client-message client step)))
    (pg--read-pkt connection (lambda (type len)
			       (pg--sasl-continue client step type len)))
    (setq step (let ((sasl-read-passphrase (lambda (_prompt) password)))
		 (sasl-next-step client step)))
    (pg--send-pkt connection ?p (lambda () (pg--insert-sasl-step step)))
    (pg--read-pkt connection (lambda (type len)
			       (pg--sasl-final client step type len)))
    (setq step (sasl-next-step client step))))

(defun pg--startup-state (connection type _len user password)
  (cl-ecase type
    (?R
     (let ((areq (pg--get-int 4)))
       (cond ((= areq pg:AUTH_REQ_MD5)
	      (let* ((salt (pg--get-bytes 4))
		     (crypted (pg--md5-encode user password salt)))
		;;(message "md5 %S %S %S => %S\n" user password salt crypted)
		(pg--send-pkt connection ?p (lambda ()
					      (pg--insert-string crypted)))
		nil))
	     ((= areq pg:AUTH_REQ_SASL)
	      (pg--read-sasl-requests connection user password)
	      nil)
	     ((= areq pg:AUTH_REQ_OK)
	      (pg--read-connection-params connection)
	      t)
	     (t (error "Authentication request not supported: %d" areq)))))
    (?E (pg--signal-error connection (pg--get-error-notice)))))

(cl-defun pg:connect (dbname user &optional
			     (password "") (host "localhost") (port 5432)
			     _encoding)
  "Initiate a connection with the PostgreSQL backend.
Connect to the database DBNAME with the username USER, on PORT of
HOST, providing PASSWORD if necessary. Return a connection to the
database (as an opaque type). PORT defaults to 5432, HOST to
\"localhost\", and PASSWORD to an empty string."
  (let* ((conn (pg--make-connection
		(lambda (buf)
		  (open-network-stream "postgres" buf host port)))))
    (pg--send-pkt conn nil (lambda () (pg--insert-startup-packet dbname user)))
    (while (not (pg--read-pkt conn (lambda (type len)
				     (pg--startup-state conn type len
							user password)))))
    (when (and (not pg:disable-type-coercion)
               (null pg--parsers))
      (pg:initialize-parsers conn))
    conn))

(defun pg--get-attributes ()
  (let* ((nfields (pg--get-int 2))
	 (attrs (make-vector nfields nil)))
    (dotimes (i nfields)
      (let ((a (pg--make-attrdesc :name (pg--get-utf8)
				  :tableid  (pg--get-int 4)
				  :columnid  (pg--get-int 2)
				  :typeid  (pg--get-int 4)
				  :typelen  (pg--get-int 2)
				  :atttypmod  (pg--get-int 4)
				  :format  (pg--get-int 2))))
	(aset attrs i a)))
    attrs))

(defun pg--parse (str oid)
  (let ((parser (assq oid pg--parsers)))
    (cond ((consp parser) (funcall (cdr parser) str))
	  (t str))))

(defun pg--get-tuple (attrs)
  (let ((nfields (pg--get-int 2))
	(stack '()))
    (cl-assert (= nfields (length attrs)))
    (dotimes (i nfields)
      (let* ((len (pg--get-int 4))
	     (bytes (pg--get-bytes len))
	     (attr (aref attrs i))
	     (typeid (pg--attrdesc-typeid attr))
	     (parsed (pg--parse bytes typeid)))
	(push parsed stack)))
    (reverse stack)))

(defun pg--read-tuples (connection attrs)
  (let* ((stack '())
	 (msg nil)
	 (f (lambda (type _len)
	      (cl-ecase type
		(?D (push (pg--get-tuple attrs) stack)
		    nil)
		(?C (setq msg (pg--get-utf8)))))))
    (while (not (pg--read-pkt connection f)))
    (pg--make-result :conn connection :tuples (reverse stack)
		     :status msg :attrs attrs)))

(defun pg--read-sync-response (connection)
  (pg--read-pkt connection (lambda (type _len)
			     (cl-ecase type
			       (?Z (cl-ecase (pg--get-char)
				     (?I ; idle
				      )
				     (?T ; in transaction
				      )
				     (?E ; error in a transaction
				      )
				     ))))))

(defun pg--read-result (connection)
  (let* ((f0 (lambda (type _len)
	       (cl-ecase type
		 (?T
		  (let ((attrs (pg--get-attributes)))
		    (pg--read-tuples connection attrs)))
		 (?C
		  (let ((msg (pg--get-utf8)))
		    (pg--make-result :conn connection :status msg)))
		 (?E
		  (let ((err (pg--get-error-notice)))
		    (pg--read-sync-response connection)
		    (pg--signal-error connection err))))))
	     (result (pg--read-pkt connection f0)))
    (pg--read-sync-response connection)
    result))

(defun pg:exec (connection &rest args)
  "Execute the SQL command given by the concatenation of ARGS
on the database to which we are connected via CONNECTION. Return
a result structure which can be decoded using `pg:result'."
  (let ((sql (apply #'concat args)))
    (pg--send-pkt connection ?Q (lambda () (pg--insert-string sql)))
    (pg--read-result connection)))

(defun pg:result (result what &rest arg)
  "Extract WHAT component of RESULT.
RESULT should be a structure obtained from a call to `pg:exec',
and the keyword WHAT should be one of
   :connection -> return the connection object
   :status -> return the status string provided by the database
   :attributes -> return the metadata, as a list of lists
   :tuples -> return the data, as a list of lists
   :tuple n -> return the nth component of the data
   :oid -> return the OID (a unique identifier generated by PostgreSQL
           for each row resulting from an insertion)"
  (cl-ecase what
    (:tuples (pg--result-tuples result))
    (:connection (pg--result-conn result))
    (:tuple (let ((which (if (integerp (car arg)) (car arg)
				(error "%s is not an integer" arg)))
		  (tuples (pg--result-tuples result)))
	      (nth which tuples)))
    (:status (pg--result-status result))
    (:attributes (pg--result-attrs result))
    (:oid
     (let ((status (pg--result-status result)))
       (cond ((string-match "^INSERT \\([0-9]+\\) " status)
	      (string-to-number (match-string 1 status)))
	     (t
	      (error "Only INSERT commands generate an oid: %s" status)))))))

(defun pg:disconnect (connection)
  "Close the database connection.
This command should be used when you have finished with the database.
It will release memory used to buffer the data transfered between
PostgreSQL and Emacs. CONNECTION should no longer be used."
  (pg--send-pkt connection ?X (lambda ()))
  (let ((proc (pg--conn-proc connection))
	(sndbuf (pg--conn-sndbuf connection)))
    (delete-process proc)
    (kill-buffer (process-buffer proc))
    (kill-buffer sndbuf)
    (setf (pg--conn-proc connection) nil)
    (setf (pg--conn-sndbuf connection) nil)))


;; type coercion support ==============================================
;;
;; When returning data from a SELECT statement, PostgreSQL starts by
;; sending some metadata describing the attributes. This information
;; is read by `pg:read-attributes', and consists of each attribute's
;; name (as a string), its size (in bytes), and its type (as an oid
;; which identifies a row in the PostgreSQL system table pg_type). Each
;; row in pg_type includes the type's name (as a string).
;;
;; We are able to parse a certain number of the PostgreSQL types (for
;; example, numeric data is converted to a numeric Emacs Lisp type,
;; dates are converted to the Emacs date representation, booleans to
;; Emacs Lisp booleans). However, there isn't a fixed mapping from a
;; type to its OID which is guaranteed to be stable across database
;; installations, so we need to build a table mapping OIDs to parser
;; functions.
;;
;; This is done by the procedure `pg:initialize-parsers', which is run
;; the first time a connection is initiated with the database from
;; this invocation of Emacs, and which issues a SELECT statement to
;; extract the required information from pg_type. This initialization
;; imposes a slight overhead on the first request, which you can avoid
;; by setting `pg:disable-type-coercion' to non-nil if it bothers you.
;; ====================================================================


;; this is a var not a const to allow user-defined types (a PostgreSQL
;; feature not present in ANSI SQL). The user can add a (type-name .
;; type-parser) pair and call `pg:initialize-parsers', after which the
;; user-defined type should be returned parsed from `pg:result'.
;; Untested.
(defvar pg:type-parsers
  `(("bool"      . ,'pg:bool-parser)
    ("char"      . ,'pg:text-parser)
    ("char2"     . ,'pg:text-parser)
    ("char4"     . ,'pg:text-parser)
    ("char8"     . ,'pg:text-parser)
    ("char16"    . ,'pg:text-parser)
    ("text"      . ,'pg:text-parser)
    ("varchar"   . ,'pg:text-parser)
    ("numeric"   . ,'pg:number-parser)
    ("int2"      . ,'pg:number-parser)
    ("int8"      . ,'pg:number-parser)
    ("int28"     . ,'pg:number-parser)
    ("int4"      . ,'pg:number-parser)
    ("oid"       . ,'pg:number-parser)
    ("float4"    . ,'pg:number-parser)
    ("float8"    . ,'pg:number-parser)
    ("money"     . ,'pg:number-parser)
    ("date"      . ,'pg:date-parser)
    ("timestamp" . ,'pg:isodate-parser)
    ("datetime"  . ,'pg:isodate-parser)
    ("time"      . ,'pg:text-parser)     ; preparsed "15:32:45"
    ;; ("reltime"   . ,'pg:text-parser)     ; don't know how to parse these
    ;; ("timespan"  . ,'pg:text-parser)
    ;; ("tinterval" . ,'pg:text-parser)
    ))

;; see `man pgbuiltin' for details on PostgreSQL builtin types
(defun pg:number-parser (str) (string-to-number str))

(defsubst pg:text-parser (str)
  (decode-coding-string str 'utf-8))

(defun pg:bool-parser (str)
  (cond ((string= "t" str) t)
        ((string= "f" str) nil)
        (t (error "Badly formed boolean from backend: %s" str))))

;; format for ISO dates is "1999-10-24"
(defun pg:date-parser (str)
  (let ((year  (string-to-number (substring str 0 4)))
        (month (string-to-number (substring str 5 7)))
        (day   (string-to-number (substring str 8 10))))
    (encode-time 0 0 0 day month year)))

(defconst pg--ISODATE_REGEX
  (concat
   "\\([0-9]\\{4\\}\\)-\\([0-9]\\{2\\}\\)-\\([0-9]\\{2\\}\\) " ; Y-M-D
   "\\([0-9]\\{2\\}\\):\\([0-9]\\{2\\}\\):\\([.0-9]+\\)" ; H:M:S.S
   "\\([-+][0-9]+\\)?")) ; TZ

;;  format for abstime/timestamp etc with ISO output syntax is
;;;    "1999-01-02 14:32:53+01"
;; which we convert to the internal Emacs date/time representation
;; (there may be a fractional seconds quantity as well, which the regex
;; handles)
(defun pg:isodate-parser (str)
  (if (string-match pg--ISODATE_REGEX str)  ; is non-null
      (let ((year    (string-to-number (match-string 1 str)))
	    (month   (string-to-number (match-string 2 str)))
	    (day     (string-to-number (match-string 3 str)))
	    (hours   (string-to-number (match-string 4 str)))
	    (minutes (string-to-number (match-string 5 str)))
	    (seconds (round (string-to-number (match-string 6 str))))
	    (tzs (when (match-string 7 str)
		   (* 3600 (string-to-number (match-string 7 str))))))
	(encode-time seconds minutes hours day month year tzs))
    (error "Badly formed ISO timestamp from backend: %s" str)))

(defun pg:initialize-parsers (connection)
  (let* ((pgtypes (pg:exec connection "SELECT typname,oid FROM pg_type"))
         (tuples (pg:result pgtypes :tuples)))
    (setq pg--parsers '())
    (mapc (lambda (tuple)
	    (apply (lambda (typname oidstring)
		     (let* ((oid (string-to-number oidstring))
			    (parser (assoc typname pg:type-parsers)))
		       (when (consp parser)
			 (push (cons oid (cdr parser)) pg--parsers))))
		   tuple))
	  tuples)))


;; md5 auth

(defun pg--md5-encode (user password salt)
  (format "md5%s" (pg--md5-key-salt (pg--md5-key-salt password user) salt)))

(defun pg--md5-key-salt (key salt)
  (let ((d (pg--md5-hex-digest (concat key salt))))
    (cl-assert (= (length d) 32))
    d))

(defun pg--md5-hex-digest (string)
  (cond ((fboundp 'md5) (md5 string))
	(t
	 (let ((tmpfile (make-temp-name "/tmp/md5-hex")))
	   (with-temp-file tmpfile (insert string))
	   (unwind-protect
	       (with-temp-buffer
		 (let ((c (call-process "md5sum" tmpfile (current-buffer))))
		   (cl-assert (zerop c))
		   (goto-char (point-min))
		   (search-forward " ")
		   (buffer-substring 1 (1- (point)))))
	     (delete-file tmpfile))))))


;; large object support ================================================
;;
;; Humphrey: Who is Large and to what does he object?
;;
;; Large objects are the PostgreSQL way of doing what most databases
;; call BLOBs (binary large objects). In addition to being able to
;; stream data to and from large objects, PostgreSQL's
;; object-relational capabilities allow the user to provide functions
;; which act on the objects.
;;
;; For example, the user can define a new type called "circle", and
;; define a C or Tcl function called `circumference' which will act on
;; circles. There is also an inheritance mechanism in PostgreSQL. 
;;
;;======================================================================
(defvar pg--lo-initialized nil)
(defvar pg--lo-functions '())

(defun pg--lo-init (connection)
  (let* ((res (pg:exec connection
                       "SELECT proname, oid FROM pg_proc WHERE proname in ("
		       "'lo_open',"
		       "'lo_close',"
                       "'lo_creat',"
                       "'lo_unlink',"
                       "'lo_lseek',"
                       "'lo_tell',"
                       "'loread',"
                       "'lowrite')")))
    (setq pg--lo-functions '())
    (mapc (lambda (tuple)
	    (push (cons (car tuple) (cadr tuple)) pg--lo-functions))
	  (pg:result res :tuples))
    (setq pg--lo-initialized t)))

(defun pg--send-function-call (connection fnid args)
  (let* ((f2 (lambda (arg)
	      (cl-etypecase arg
		(integer
		 (pg--insert-int 4 4)
		 (pg--insert-int arg 4))
		(string
		 (let ((bytes (encode-coding-string arg 'utf-8 t)))
		   (pg--insert-int (length bytes) 4)
		   (pg--insert-bytes bytes))))))
	 (f (lambda ()
	      (pg--insert-int fnid 4)
	      (pg--insert-int 1 2)
	      (pg--insert-int 1 2)
	      (pg--insert-int (length args) 2)
	      (mapc f2 args)
	      (pg--insert-int 1 2))))
    (pg--send-pkt connection ?F f)))

(defun pg--read-function-result (connection integer-result?)
  (let* ((f (lambda (type _len)
	      (cl-ecase type
		(?V (let* ((len (pg--get-int 4)))
		      (cond (integer-result? (pg--get-int len))
			    (t (pg--get-bytes len))))))))
	 (result nil))
    (while (not (setq result (pg--read-pkt connection f))))
    ;;(message "function result => %S" result)
    (pg--read-pkt connection (lambda (type _len)
			       (cl-case type
				 (?Z (cl-case (pg--get-char)
				       (?T))))))
    result))

;; fn is either an integer, in which case it is the OID of an element
;; in the pg_proc table, and otherwise it is a string which we look up
;; in the alist `pg--lo-functions' to find the corresponding OID.
(defun pg--fn (connection fn integer-result? &rest args)
  (or pg--lo-initialized (pg--lo-init connection))
  (let ((fnid (cl-etypecase fn
		(integer fn)
		(string (or (cdr (assoc fn pg--lo-functions))
			    (error "Unknown builtin function: %S" fn))))))
    ;;(message "sending call: %S %S" fnid args)
    (pg--send-function-call connection fnid args)
    (pg--read-function-result connection integer-result?)))

(defconst pg:INV_ARCHIVE 65536)         ; fe-lobj.c
(defconst pg:INV_WRITE   131072)
(defconst pg:INV_READ    262144)

;; returns an OID
(defun pg:lo-create (connection &optional args)
  (let* ((modestr (or args "r"))
         (mode (cond ((integerp modestr) modestr)
		     ((string= "r" modestr) pg:INV_READ)
                     ((string= "w" modestr) pg:INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg:INV_READ pg:INV_WRITE))
                     (t (error "pg:lo-create: bad mode %s" modestr))))
         (oid (pg--fn connection "lo_creat" t mode)))
    (cond ((not (integerp oid))
           (error "Didn't return an OID: %S" oid))
          ((zerop oid)
           (error "Can't create large object"))
          (t oid))))

;; args = modestring (default "r", or "w" or "rw")
;; returns a file descriptor for use in later pg:lo-* procedures
(defun pg:lo-open (connection oid &optional args)
  (let* ((modestr (or args "r"))
         (mode (cond ((integerp modestr) modestr)
		     ((string= "r" modestr) pg:INV_READ)
                     ((string= "w" modestr) pg:INV_WRITE)
                     ((string= "rw" modestr)
                      (logior pg:INV_READ pg:INV_WRITE))
                     (t (error "pg:lo-open: bad mode %s" modestr))))
         (fd (pg--fn connection "lo_open" t oid mode)))
    (unless (integerp fd)
      (error "Couldn't open large object"))
    fd))

(defun pg:lo-close (connection fd)
  (pg--fn connection "lo_close" t fd))

(defun pg:lo-read (connection fd bytes)
  (pg--fn connection "loread" nil fd bytes))

(defun pg:lo-write (connection fd buf)
  (pg--fn connection "lowrite" t fd buf))

(defun pg:lo-lseek (connection fd offset whence)
  (pg--fn connection "lo_lseek" t fd offset whence))

(defun pg:lo-tell (connection oid)
  (pg--fn connection "lo_tell" t oid))

(defun pg:lo-unlink (connection oid)
  (pg--fn connection "lo_unlink" t oid))

;; returns an OID
;; FIXME should use unwind-protect here
(defun pg:lo-import (connection filename)
  (let* ((oid (pg:lo-create connection "rw"))
         (fdout (pg:lo-open connection oid "w")))
    (unwind-protect
	(with-temp-buffer
	  (insert-file-contents-literally filename)
	  (let ((pos (point-min)))
	    (while (< pos (point-max))
	      (let ((str (buffer-substring-no-properties
			  pos (min (point-max) (cl-incf pos 1024)))))
		(pg:lo-write connection fdout str)))))
      (pg:lo-close connection fdout))
    oid))

(defun pg:lo-export (connection oid filename)
  (with-temp-file filename
    (let* ((fdin (pg:lo-open connection oid "r")))
      (cl-do ((str (pg:lo-read connection fdin 1024)
                   (pg:lo-read connection fdin 1024)))
          ((or (not str) (zerop (length str))))
        (insert str))
      (pg:lo-close connection fdin))))


;; DBMS metainformation ================================================
;;
;; Metainformation such as the list of databases present in the
;; database management system, list of tables, attributes per table.
;; This information is not available directly, but can be deduced by
;; querying the system tables.
;;
;; Based on the queries issued by psql in response to user commands
;; `\d' and `\d tablename'; see file
;; /usr/local/src/pgsql/src/bin/psql/psql.c
;; =====================================================================
(defun pg:databases (conn)
  "Return a list of the databases available at this site."
  (let ((res (pg:exec conn "SELECT datname FROM pg_database")))
    (apply #'append (pg:result res :tuples))))

(defun pg:tables (conn)
  "Return a list of the tables present in this database."
  (let ((res (pg:exec
	      conn "SELECT relname FROM pg_class, pg_user WHERE "
              "(relkind = 'r' OR relkind = 'i' OR relkind = 'S') AND "
              "relname !~ '^pg_' AND usesysid = relowner ORDER BY relname")))
    (apply #'append (pg:result res :tuples))))

(defun pg:columns (conn table)
  "Return a list of the columns present in TABLE."
  (let* ((sql (format "SELECT * FROM %s WHERE 0 = 1" table))
         (res (pg:exec conn sql)))
    (mapcar #'car (pg:result res :attributes))))

(defun pg:backend-version (conn)
  "Version an operating environment of the backend as a string."
  (let ((res (pg:exec conn "SELECT version()")))
    (car (pg:result res :tuple 0))))



;; this is ugly because lambda lists don't do destructuring
(defmacro with-pg-connection (con open-args &rest body)
  "Bindspec is of the form (connection open-args), where OPEN-ARGS are
as for PG:CONNECT. The database connection is bound to the variable
CONNECTION. If the connection is unsuccessful, the forms are not
evaluated. Otherwise, the BODY forms are executed, and upon
termination, normal or otherwise, the database connection is closed."
  `(let ((,con (pg:connect ,@open-args)))
     (unwind-protect
         (progn ,@body)
       (when ,con (pg:disconnect ,con)))))

(defun pg--with-transaction (conn fun)
  (pg:exec conn "START TRANSACTION")
  (let ((committed? nil))
    (unwind-protect
	(prog1 (funcall fun)
          (pg:exec conn "COMMIT")
	  (setq committed? t))
      (unless committed?
	(pg:exec conn "ROLLBACK")))))

(defmacro with-pg-transaction (con &rest body)
  "Execute BODY forms in a BEGIN..END block.
If a PostgreSQL error occurs during execution of the forms, execute
a ROLLBACK command.
Large-object manipulations _must_ occur within a transaction, since
the large object descriptors are only valid within the context of a
transaction."
  (declare (indent 1))
  `(pg--with-transaction ,con (lambda () ,@body)))

(defun pg:for-each (conn select-form callback)
  "Create a cursor for SELECT-FORM, and call CALLBACK for each result.
Uses the PostgreSQL database connection CONN. SELECT-FORM must be an
SQL SELECT statement. The cursor is created using an SQL DECLARE
CURSOR command, then results are fetched successively until no results
are left. The cursor is then closed.

The work is performed within a transaction. The work can be
interrupted before all tuples have been handled by THROWing to a tag
called ’pg-finished."
  (let ((cursor (symbol-name (gensym "pgelcursor"))))
    (catch 'pg-finished
      (with-pg-transaction conn
	(pg:exec conn "DECLARE " cursor " CURSOR FOR " select-form)
        (unwind-protect
	    (let ((res nil))
              (while (setq res (pg:result (pg:exec conn "FETCH 1 FROM " cursor)
					  :tuples))
		(funcall callback res)))
          (pg:exec conn "CLOSE " cursor))))))

(provide 'pg)

;;; pg.el ends here
