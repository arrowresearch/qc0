init:
	@w build
	@w service postgresql start
	@w link python pip poetry black ipython
	@w poetry install
	@w psql -1 -f ./db.sql

test:
	@pytest
