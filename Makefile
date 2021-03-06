PYTEST_ARGS = --doctest-glob='*.rst'
PYTEST_COV_ARGS = --cov qc0 --cov-report term-missing

init:
	@w build
	@w service postgresql start
	@w link python pip poetry black ipython cloc qc0-shell pytest flake8
	@w poetry install
	@w psql -1 -f ./db.sql
	@w commit

up:
	@w up
	@w service postgresql start

down:
	@w down

lint:
	@flake8 qc0/ tests/

test:
	@pytest $(PYTEST_ARGS)

test-cov:
	@pytest $(PYTEST_ARGS)  $(PYTEST_COV_ARGS)

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
