.PHONY: install dev test lint seed simulate loadtest replay docker clean

install:
	python -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt

dev:
	.venv/bin/uvicorn app.main:app --reload --port 8000

test:
	.venv/bin/python -m pytest -q

lint:
	.venv/bin/ruff check .

seed:
	.venv/bin/python scripts/seed_plant.py

simulate:
	.venv/bin/python scripts/simulate_sensors.py --rate 250 --seconds 60 --post

loadtest:
	.venv/bin/python scripts/run_loadtest.py --rate 250 --seconds 60

replay:
	.venv/bin/python scripts/replay_test.py --attempts 5000

docker:
	docker compose up --build

clean:
	rm -rf .pytest_cache .ruff_cache data *.db *.db-wal *.db-shm
	find . -name __pycache__ -type d -exec rm -rf {} +
