init:
	@w build
	@w service postgresql start
	@w link python pip poetry black ipython cloc qc0-shell
	@w poetry install
	@w psql -1 -f ./db.sql
	@w commit

up:
	@w up
	@w service postgresql start

down:
	@w down

test:
	@pytest

test-cov:
	@pytest --cov qc0 --cov-report term-missing

fmt-check:
	@black --check qc0/ tests/

fmt:
	@black qc0/ tests/

cloc:
	@cloc --by-file qc0
	@cloc --by-file tests/*.py

ci-test:
	service postgresql start
	psql -1 -f ./db.sql
	poetry install
	$(MAKE) fmt-check
	$(MAKE) test-cov
	$(MAKE) cloc
