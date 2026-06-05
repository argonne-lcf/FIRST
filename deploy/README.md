# Docker Deployment

This directory contains all the necessary files for deploying the FIRST Inference Gateway using Docker.

## Files

- `docker-compose.yml` - Docker Compose configuration for multi-container deployment
- `Dockerfile` - Container image definition for the gateway application
- `nginx.conf` - Nginx reverse proxy configuration
- `env.example` - Example environment variables file

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+

## Quick Start

**Note**: All commands should be run from this directory (`deploy/docker/`)

1. Copy the example environment file:
   ```bash
   cp env.example .env
   ```

2. Edit `.env` with your configuration:
   - Set a strong `SECRET_KEY` (generate with: `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"`)
   - **For development**: Keep `RUNNING_AUTOMATED_TEST_SUITE=True` to skip Globus policy checks
   - **For production**: Set `RUNNING_AUTOMATED_TEST_SUITE=False` and configure:
     - Globus credentials (`GLOBUS_APPLICATION_ID`, `GLOBUS_APPLICATION_SECRET`, etc.)
     - Globus policies (`GLOBUS_POLICIES`)
     - Authorized IDP domains (`AUTHORIZED_IDP_DOMAINS`)
   - Set `LOG_TO_STDOUT=True` so application logs are visible via `docker-compose logs`
   - Adjust database credentials if needed
   - Update `ALLOWED_HOSTS` for your domain

3. Start the services:
   ```bash
   docker-compose up -d
   ```

4. Initialize the database:
   ```bash
   docker-compose exec inference-gateway python manage.py migrate
   ```

## Accessing the Gateway

- Gateway API: http://localhost:8000

## Managing the Services

- **View logs**: `docker-compose logs -f`
- **Stop services**: `docker-compose down`
- **Rebuild after code changes**: `docker-compose up -d --build`
- **View running containers**: `docker-compose ps`

## Troubleshooting

### Application Not Starting / "Globus High Assurance Policy" Error
If the container exits immediately with no logs, or you see an error about Globus policies:
```bash
docker-compose logs inference-gateway
```

**Solution**: Ensure your `.env` file has `RUNNING_AUTOMATED_TEST_SUITE=True` for development, or configure proper Globus credentials for production.

### Database Connection Issues
If you see database connection errors, ensure PostgreSQL is fully initialized:
```bash
docker-compose logs postgres
```

### View Application Logs
To see detailed application logs:
```bash
# Follow all logs
docker-compose logs -f

# View only gateway logs
docker-compose logs -f inference-gateway

# Check if container is running
docker-compose ps
```

### Rebuilding from Scratch
To completely reset the deployment:
```bash
docker-compose down -v  # Warning: This deletes all data!
docker-compose up -d --build
```

## Production Considerations

For production deployments:
1. Set `DEBUG=False` in `.env`
2. Use a strong, randomly generated `SECRET_KEY`
3. Configure proper `ALLOWED_HOSTS`
4. Set up SSL/TLS termination (use nginx with SSL certificates)
5. Configure proper backup strategies for PostgreSQL data
6. Use Docker secrets for sensitive credentials

For detailed instructions, see the [Docker Deployment Guide](../../docs/admin-guide/gateway-setup/docker.md).

