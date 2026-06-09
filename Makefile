.PHONY: up down

up:
	docker compose up --build
down:
	docker compose down
