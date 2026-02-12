.DEFAULT: help

compose_file = docker-compose.yml
compose_command = docker-compose --file $(compose_file)

help:
	@echo "make help"

format:
	ruff format

unit:
	python -m pytest --cov-report term-missing --cov=. tests/unit

functional:
	python -m pytest tests/functional --env=$(ENVIRONMENT) -n=4

integration:
	python -m pytest tests/integration --env=$(ENVIRONMENT) -n=4

up:
	$(compose_command) up -d

up-watch:
	$(compose_command) up

down:
	$(compose_command) down

# deletes data stored for the local services (postgres, ES, ...)
down-clean:
	$(compose_command) down --volumes

stop:
	$(compose_command) stop

seed:
	docker compose -f docker-compose.setup.yml up --build --abort-on-container-exit --exit-code-from devsetup
	echo "\n✅ Local environment setup complete." # will not display if previous line does not exit 0
