SHELL := /bin/bash

setup: clean install-commit-hooks install-tox dev
reset: setup
install-commit-hooks:
	pip3 install "pre-commit>=2.7"
	pre-commit install
install-tox:
	pip3 install "tox>=3.20"
dev:
	tox --notest -e dev
start-dev-kafka-cluster: stop-dev-kafka-cluster
	docker-compose run -d --name=kafka -p "9092:9092" kafka
	docker-compose run -d --no-deps --name=registry -p "8081:8081" registry
stop-dev-kafka-cluster:
	docker stop kafka registry || true
	docker rm kafka registry || true
	docker-compose stop
test:
	./devenv pytest
unit-test:
	./devenv pytest tests/unit
integration-test:
	./devenv pytest tests/integration
fmt:
	./devenv isort .
	./devenv black .
lint:
	./devenv isort . --check --diff
	./devenv black --check .
	./devenv pydocstyle
	./devenv flake8
	./devenv bandit -r src
	./devenv mypy
prepare: fmt lint test
clean-commit-hooks:
	! which pre-commit || pre-commit uninstall
clean-docker:
	docker-compose down
clean-pycache:
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
clean-pytest:
	rm -rf .pytest_cache
clean-mypy:
	rm -rf .mypy_cache
clean-tox:
	rm -rf .eggs .tox musekafka.egg-info
clean: clean-commit-hooks clean-docker clean-pycache clean-pytest clean-mypy clean-tox


# These commands are for use on master only, as part of the release process.
release-major:
	./release major
release-minor:
	./release minor
release-patch:
	./release patch
