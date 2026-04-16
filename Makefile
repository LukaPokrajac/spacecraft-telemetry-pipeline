.PHONY: setup env up down generator consumer dashboard spark clean logs

setup:
	python -m venv .venv
	.venv/bin/pip install -r requirements.txt

env:
	cp .env.example .env

up:
	docker compose up -d

down:
	docker compose down

generator:
	.venv/bin/python src/generator.py

consumer:
	.venv/bin/python src/consumer.py

dashboard:
	.venv/bin/streamlit run src/dashboard.py

spark:
	.venv/bin/python src/spark_job.py

logs:
	docker compose logs -f

clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
