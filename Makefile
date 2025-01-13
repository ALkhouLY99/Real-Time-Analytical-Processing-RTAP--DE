# Default target
all: help

# Help: Display available targets
help:
	@echo "Available targets:"
	@echo "  up       - Start the Docker Compose services in detached mode"
	@echo "  stop     - Stop the Docker Compose services"
	@echo "  restart  - Restart the Docker Compose services"
	@echo "  down     - Stop and remove the Docker Compose services"
	@echo "  logs     - View logs for all services"
	@echo "  ps       - List running containers"
	@echo "  build    - Build or rebuild services"
	@echo "  clean    - Stop and remove containers, networks, and images"

# Start the Docker Compose services in detached mode
up:
	docker-compose up -d

# Stop the Docker Compose services
stop:
	docker-compose stop

# Restart the Docker Compose services
restart: stop up

# Stop and remove the Docker Compose services
down:
	docker-compose down

# View logs for all services
logs:
	docker-compose logs -f

# List running containers
ps:
	docker-compose ps

# Build or rebuild services
build:
	docker-compose build

# Stop and remove containers, networks, and images
clean: down
	docker-compose rm -f
	docker system prune -f

# Display help by default
.PHONY: all help up stop restart down logs ps build clean