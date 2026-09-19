# ---------------------------------------------------------------------------
# CDC Data Lake — developer & CI automation.
#
# Local note: on macOS a system Spark on PATH can shadow the pinned pyspark and
# an incompatible JDK breaks Spark 3.5. `make test` therefore unsets SPARK_HOME
# and, if openjdk@17 is present, uses it. CI uses a clean runner so it just runs
# pytest directly.
# ---------------------------------------------------------------------------
VENV        ?= .venv
PY          ?= $(VENV)/bin/python
PIP         ?= $(VENV)/bin/pip
DIST        ?= dist
JAVA17      ?= /opt/homebrew/opt/openjdk@17

.PHONY: help venv install test lint fmt build clean tf-fmt tf-validate

help:
	@echo "Targets: venv install test lint fmt build clean tf-fmt tf-validate"

venv:
	python3.11 -m venv $(VENV) || python3 -m venv $(VENV)

install: venv
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -r requirements-dev.txt

# Run the unit test suite. Overridable JAVA_HOME for Spark compatibility.
test:
	@JH="$${JAVA_HOME}"; \
	if [ -d "$(JAVA17)" ]; then JH="$(JAVA17)"; fi; \
	env -u SPARK_HOME SPARK_LOCAL_IP=127.0.0.1 JAVA_HOME="$$JH" \
	    PATH="$$JH/bin:$$PATH" $(PY) -m pytest tests/ -q

lint:
	$(VENV)/bin/ruff check glue tests lambda
	$(VENV)/bin/black --check glue tests lambda

fmt:
	$(VENV)/bin/ruff check --fix glue tests lambda
	$(VENV)/bin/black glue tests lambda

# Build the shared library zip that Glue jobs load via --extra-py-files.
# The zip contains the `cdc_lib` package at its root.
build: clean
	mkdir -p $(DIST)
	cd glue && zip -q -r ../$(DIST)/cdc_lib.zip cdc_lib -x '*__pycache__*'
	@echo "built $(DIST)/cdc_lib.zip"

clean:
	rm -rf $(DIST)/cdc_lib.zip
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +

tf-fmt:
	cd terraform && terraform fmt -recursive

tf-validate: build
	cd terraform && terraform init -backend=false && terraform validate
