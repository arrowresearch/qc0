init:
	@w build
	@w service postgresql start
	@w link python pip poetry black ipython
	@w poetry install
	@w psql -1 -f ./db.sql
	@w commit

up down:
	@w up

test:
	@pytest

fmt:
	@black qc0/ tests/
