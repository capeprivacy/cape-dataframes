# Global Variables used across many different rule types

# Definition of the default rule
all: test
.PHONY: all

# ###############################################
# Bootstrapping
#
# Rules for bootstrapping the Makefile such as checking for docker, python versions, etc.
# ###############################################
DOCKER_REQUIRED_VERSION=18.
SHELL := /bin/bash

CURRENT_DIR=$(shell pwd)
PIP_PATH=$(shell which pip)
DOCKER_PATH=$(shell which docker)

# Default platform
# PYPI doesn't allow linux build tags to be pushed and doesn't support
# specific operating systems such a ubuntu. It only allows build tags for linux
# to be pushed as manylinux.
DEFAULT_PLATFORM=manylinux1_x86_64

dockercheck:
ifeq (,$(DOCKER_PATH))
ifeq (,$(findstring $(DOCKER_REQUIRED_VERSION),$(shell docker version)))
ifeq (,$(BYPASS_DOCKER_CHECK))
	$(error "Docker version $(DOCKER_REQUIRED_VERSION) is required.")
endif
endif
endif

pydep-upgrade:
	pip install -U pip-tools
	CUSTOM_COMPILE_COMMAND="make pydep-upgrade" pip-compile --output-file requirements/base.txt requirements/base.in --resolver=backtracking
	CUSTOM_COMPILE_COMMAND="make pydep-upgrade" pip-compile --output-file requirements/spark.txt requirements/spark.in --resolver=backtracking
	CUSTOM_COMPILE_COMMAND="make pydep-upgrade" pip-compile --output-file requirements/dev.txt requirements/dev.in --resolver=backtracking
	pip install -r requirements/base.txt -r requirements/spark.txt -r requirements/dev.txt


pydep:
	pip install -r requirements/base.txt -r requirements/spark.txt -r requirements/dev.txt

bootstrap:
	pip install -U pip setuptools
	pip install -r requirements/base.txt -r requirements/spark.txt
	pip install -e .

# ###############################################
# Testing and Linting
#
# Rules for running our tests and for running various different linters
# ###############################################
test:
	pytest

CI_FILES=cape_privacy/pandas cape_privacy/spark cape_privacy/policy cape_privacy/coordinator

lint:
	flake8 .

ci: lint test coverage

fmt:
	isort --atomic .
	black .

coverage:
	pytest --cov-report=xml --cov=cape_privacy ${CI_FILES}
	coverage report --fail-under=90

examples:
	shopt -s nullglob; \
	for dir in examples examples/tutorials; do \
		pushd $$dir; \
		for i in *.py; do \
			line=$$(head -n 1 $$i); \
			if [[ $$line == "# SKIP_CI" ]]; then \
			  continue; \
			fi; \
			echo "Running $$i"; \
			python $$i || exit 1; \
		done; \
		popd; \
	done;

docker:
	docker build -t capeprivacy/cape-dataframes .

.PHONY: lint fmt test coverage examples
