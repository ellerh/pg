# SPDX-License-Identifier: GPL-3.0-or-later

PASSWORD=
EMACS=emacs

# Extra authentication methods to test.  This usually requires
# configuration in pg_hba.conf.  The value is a list of triples.Eg.:
# AUTHMETHODS=(("template1" "pgeltrust" "") ("template1" "pgelmd5" "md5secret") ("template1" "pgelscram" "scramsecret"))
AUTHMETHODS=

all: pg.elc pg-test.elc

check: pg.elc pg-test.elc
	$(EMACS) -Q --batch -L . -l pg-test \
	--eval '(setq pg-test-password "$(PASSWORD)")' \
	--eval '(setq pg-test-authmethods (quote $(AUTHMETHODS)))' \
	-f ert-run-tests-batch

%.elc: %.el
	$(EMACS) -Q --batch -L . -f batch-byte-compile $^

clean:
	find . -maxdepth 1 -name '*.elc' -exec rm -v -- {} +
