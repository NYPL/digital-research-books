.DEFAULT: help

compose_file = docker-compose.yml
compose_command = docker-compose --file $(compose_file)

help:
	@echo "make help"

unit:
	python -m pytest --cov-report term-missing --cov=. tests/unit

functional:
	python -m pytest tests/functional --env=$(ENV)

integration:
	python -m pytest tests/integration --env=$(ENV)

up:
	$(compose_command) up -d

up-watch:
	$(compose_command) up

down:
	$(compose_command) down

down-clean:
	$(compose_command) down --volumes

stop:
	$(compose_command) stop
