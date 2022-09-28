;; pg-test.el --- Tests for pg.el               -*- lexical-binding: t -*-
;;
;; SPDX-License-Identifier: GPL-3.0-or-later
;; Copyright: Helmut Eller
;;
;; This (limited) testing code assumes you have a database user
;; "postgres" with no password accessible from the localhost, and
;; a database named "template1". It should clean up after itself.
;;

(require 'pg)
(require 'ert)

(ert-deftest pg-test-get-char ()
  (let ((buf (generate-new-buffer "foo")))
    (with-current-buffer buf
      (insert "abc")
      (should (= (pg--get-char) ?a))
      (should (= (pg--get-char) ?b))
      (should (= (pg--get-char) ?c))
      (should-error (pg--get-char)))))

(ert-deftest pg-test-get-int ()
  (let ((buf (generate-new-buffer "foo")))
    (with-current-buffer buf
      (set-buffer-multibyte nil)
      (insert 0 0 0 1)
      (should (= (pg--get-int 4) 1))
      (should (zerop (buffer-size)))
      (insert #xff #xff #xff #xff)
      (should (= (pg--get-int 4) -1))
      (insert #x0a #xbc #xcd #xef)
      (should (= (pg--get-int 4) #xabccdef))
      (insert #x80 0 0 1)
      (should (= (pg--get-int 4) #x-7fffffff)))))

(ert-deftest pg-test-read-pkt ()
  (let* ((conn (pg--make-connection
		(lambda (buf)
		  (let* ((fstr "\\x03\\x00\\x00\\x00\\x08abcdefg")
			 (proc (start-process "foo" buf "printf" fstr)))
		    (set-process-sentinel proc (lambda (_ _) ))
		    proc)))))
    (pg--read-pkt conn
		  (lambda (type len)
		    (should (= type 3))
		    (should (= len 8))
		    (should (equal (buffer-string) "abcd"))))))

(defvar pg-test-password "")

(defun pg-test--connect ()
  (pg:connect "template1" "postgres" pg-test-password))

(ert-deftest pg-test-connect ()
  (let ((pg:disable-type-coercion t))
    (let* ((c (pg-test--connect)))
      (should (pg--conn-pid c))
      (should (pg--conn-secret c))
      (should (pg--conn-params c)))
    (should-error (pg:connect "template1" "postgres" "invalid password")
		  :type 'pg-error)))

;; A list of (db user password) triples to connect to.
(defvar pg-test-authmethods '())

(ert-deftest pg-test-authmethods ()
  (let ((pg:disable-type-coercion t))
    (mapc (lambda (triple)
	    (apply (lambda (db user passwd)
		     (let* ((c (pg:connect db user passwd)))
		       (should (pg--conn-pid c))))
		   triple))
	  pg-test-authmethods)))

(ert-deftest pg-test-query ()
  (let ((pg:disable-type-coercion t)
	(pg--parsers nil))
    (let* ((c (pg-test--connect)))
      (let ((r (pg:exec c "select 1 union select 2")))
	(should (equal (pg:result r :tuples) '(("1") ("2")))))
      (let* ((r (pg:exec c "SELECT typname,oid FROM pg_type")))
	(should (assoc "varchar" (pg:result r :tuples))))))
  (let* ((c (pg-test--connect)))
    (let ((r (pg:exec c "select 1 union select 2")))
      (should (equal (pg:result r :tuples) '((1) (2)))))))

(ert-deftest pg-test-statement ()
  (let ((pg:disable-type-coercion t)
	(pg--parsers nil))
    (let* ((c (pg-test--connect)))
      (let ((r (pg:exec c "create temp table foo ()")))
	(should (equal (pg:result r :tuples) '()))
	(should (equal (pg:result r :status) "CREATE TABLE"))
	(setq r (pg:exec c "insert into foo default values"))
	(should (equal (pg:result r :status) "INSERT 0 1"))
	(setq r (pg:exec c "insert into foo default values"))
	(should (equal (pg:result r :status) "INSERT 0 1"))))))

(ert-deftest pg-test-multiple-inserts ()
  (let* ((conn (pg-test--connect)))
    (pg:exec conn "CREATE TEMP TABLE count_test(key int, val int)")
    (cl-loop for i from 1 to 100
             for sql = (format "INSERT INTO count_test VALUES(%s, %s)"
			       i (* i i))
             do (pg:exec conn sql))
    (let ((res (pg:exec conn "SELECT count(val) FROM count_test")))
      (should (= 100 (car (pg:result res :tuple 0))))
      (setq res (pg:exec conn "SELECT sum(key) FROM count_test"))
      (should (= 5050 (car (pg:result res :tuple 0))))
      (pg:exec conn "DROP TABLE count_test"))))

(ert-deftest pg-test-error ()
  (let* ((pg:disable-type-coercion t)
	 (pg--parsers nil)
	 (conn (pg-test--connect)))
    (should-error (pg:exec conn "CREATE TEMP TABLE foo(col invalid_type_name)")
		  :type 'pg-error)
    (should-error (pg:exec conn "asf")
		  :type 'pg-error)))

(ert-deftest pg-test-utf8-strings ()
  (let* ((conn (pg-test--connect)))
    (should (equal (pg:result (pg:exec conn "select 'aα𝐴'::text") :tuples)
		   '(("aα𝐴"))))
    (let ((r (pg:exec conn "select 1 as aα𝐴")))
      (should (equal (pg--attrdesc-name (aref (pg:result r :attributes) 0))
		     "aα𝐴")))))

(ert-deftest pg-test-date-parsing ()
  (let* ((conn (pg-test--connect)))
    (pg:exec conn "CREATE TEMP TABLE "
	     " date_test(a timestamp, b date, c time)")
    (pg:exec conn "INSERT INTO date_test VALUES "
             "(make_timestamp(2013, 7, 15, 8, 15, 23.5),"
	     " make_date(2013, 7, 16),"
	     " make_time(8, 15, 23.5))")
    (let ((res (pg:exec conn "SELECT * FROM date_test")))
      (cl-destructuring-bind ((a b c)) (pg:result res :tuples)
	(should (equal (format-time-string "%F %T" a) "2013-07-15 08:15:24"))
	(should (equal (format-time-string "%F %T" b) "2013-07-16 00:00:00"))
	(should (equal c "08:15:23.5"))))
    (pg:exec conn "DROP TABLE date_test")))

(ert-deftest pg-test-pg:result ()
  (let* ((c (pg-test--connect))
	 (r1 (pg:exec c "CREATE TEMP TABLE resulttest (a int, b VARCHAR(4))"))
         (r2 (pg:exec c "INSERT INTO resulttest VALUES (3, 'zae')"))
         (_r3 (pg:exec c "INSERT INTO resulttest VALUES (66, 'poiu')"))
         (r4 (pg:exec c "SELECT * FROM resulttest"))
         (r5 (pg:exec c "DROP TABLE resulttest")))
    (should (equal (pg:result r1 :status) "CREATE TABLE"))
    (should (equal (pg:result r2 :status) "INSERT 0 1"))
    (should (equal (pg:result r2 :oid) 0))
    (should (equal (pg:result r4 :status) "SELECT 2"))
    (should (equal (length (pg:result r4 :attributes)) 2))
    (should (equal (pg:result r4 :tuples) '((3 "zae") (66 "poiu"))))
    (should (equal (pg:result r4 :tuple 1) '(66 "poiu")))
    (should (equal (pg:result r5 :status) "DROP TABLE"))))

(ert-deftest pg-test-disconnect ()
  (let ((pg:disable-type-coercion t))
    (let* ((c (pg-test--connect))
	   (proc (pg--conn-proc c))
	   (buf (process-buffer proc))
	   (sndbuf (pg--conn-sndbuf c)))
      (should (equal (process-status proc) 'open))
      (pg:disconnect c)
      (should (equal (process-status proc) 'closed))
      (should (not (buffer-live-p buf)))
      (should (not (buffer-live-p sndbuf))))))

(ert-deftest pg-test-transaction ()
  (let* ((c (pg-test--connect)))
    (pg:exec c "start transaction")
    (pg:exec c "commit")

    (let ((r (with-pg-transaction c
	       (pg:exec c "select 123"))))
      (should (equal (pg:result r :tuples) '((123)))))

    (pg:exec c "create temp table mytable(k int)")
    (pg:exec c "insert into mytable values(1)")
    (should-error
     (with-pg-transaction c
       (pg:exec c "insert into mytable values (2),(3)")
       (should (equal
		(pg:result (pg:exec c "select * from mytable order by k")
			   :tuples)
		'((1) (2) (3))))
       (pg:exec c "frob"))
     :type 'pg-error)
    (should (equal (pg:result (pg:exec c "select * from mytable") :tuples)
		   '((1))))))

(ert-deftest pg-test-lo-create ()
  (let* ((conn (pg-test--connect)))
   (with-pg-transaction conn
     (let* ((oid (pg:lo-create conn "rw")))
       (should (integerp oid))
       (should (= (pg:lo-unlink conn oid) 1))))))

(ert-deftest pg-test-lo-read ()
  (let* ((conn (pg-test--connect)))
    (with-pg-transaction conn
      (let* ((oid (pg:lo-create conn "rw"))
	     (fd (pg:lo-open conn oid "rw")))
	(should (= (pg:lo-write conn fd "Hi there mate") 13))
	(should (= (pg:lo-lseek conn fd 3 0) 3))           ; SEEK_SET = 0
	(should (= 3 (pg:lo-tell conn fd)))
	(should (equal (pg:lo-read conn fd 7) "there m"))
	(should (= (pg:lo-close conn fd) 0))
	(should (= (pg:lo-unlink conn oid) 1))))))

(ert-deftest pg-test-lo-import ()
  (let* ((conn (pg-test--connect)))
    (with-pg-transaction conn
      (let ((oid (pg:lo-import conn "/etc/group")))
	(should (= (pg:lo-export conn oid "/tmp/group") 0))
	(should (zerop (call-process "diff" nil nil nil
				     "/tmp/group" "/etc/group")))
	(should (= (pg:lo-unlink conn oid) 1))
	(delete-file "/tmp/group")))))
