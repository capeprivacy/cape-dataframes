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

PYTHON_MATCHED := $(shell [[ `python -V 2>&1` =~ 3.[6-8] ]] && echo matched)

pythoncheck:
ifndef PYTHON_MATCHED
ifeq (,$(BYPASS_PYTHON_CHECK))
	$(error "Python version 3.6+ is required.")
endif
endif

pipcheck:
ifeq (,$(PIP_PATH))
ifeq (,$(BYPASS_PIP_CHECK))
	$(error "Pip must be installed")
endif
endif

bootstrap: pythoncheck pipcheck
	pip install -U pip setuptools
	pip install -r requirements.txt
	pip install -e .

# ###############################################
# Testing and Linting
#
# Rules for running our tests and for running various different linters
# ###############################################
test: pythoncheck
	pytest

CI_FILES=cape_privacy/pandas cape_privacy/spark cape_privacy/coordinator cape_privacy/utils cape_privacy/policy cape_privacy/auth

lint: pythoncheck
	flake8 ${CI_FILES}

ci: lint test coverage

fmt: pythoncheck
	isort --atomic --recursive ${CI_FILES}
	black ${CI_FILES}

coverage:
	pytest --cov-report=xml --cov=cape_privacy ${CI_FILES}
	coverage report --fail-under=90

.PHONY: lint fmt test coverage
