init:
	@w build
	@w service postgresql start
	@w link python pip poetry black ipython qc0-shell
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

fmt:
	@black qc0/ tests/

ci-test:
	service postgresql start
	psql -1 -f ./db.sql
	poetry install
	pytest
