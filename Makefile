.PHONY: all docs install format lint release test test-ci


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d '"' -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/assets/VERSION)
LEAD := $(shell head -n 1 LEAD.md)


all:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

docs:
	python scripts/docs.py
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/issue_template.md
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/pull_request_template.md

format:
	black $(PACKAGE) tests

install:
	pip install --upgrade -e .[aws,bigquery,ckan,dataflows,elastic,gsheet,html,ods,pandas,server,spss,sql,tsv,dev]
	test -f '.git/hooks/pre-commit' || cp .gitverify .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit

lint:
	black $(PACKAGE) tests --check
	pylama $(PACKAGE) tests
	# mypy $(PACKAGE) --ignore-missing-imports

release:
	git checkout master && git pull origin && git fetch -p
	@git log --pretty=format:"%C(yellow)%h%Creset %s%Cgreen%d" --reverse -20
	@echo "\nReleasing v$(VERSION) in 10 seconds. Press <CTRL+C> to abort\n" && sleep 10
	git commit -a -m 'v$(VERSION)' && git tag -a v$(VERSION) -m 'v$(VERSION)'
	git push --follow-tags

test:
	make lint
	pytest --cov ${PACKAGE} --cov-report term-missing --cov-fail-under 50

test-ci:
	make lint
	pytest --cov ${PACKAGE} --cov-report term-missing --cov-fail-under 50 --ci
