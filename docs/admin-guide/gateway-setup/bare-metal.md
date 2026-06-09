# Bare Metal Setup

This guide covers installing the FIRST Inference Gateway directly on your server without Docker.

## Prerequisites

- Linux server (Ubuntu 20.04+, CentOS 8+, or similar)
- Python 3.12 or later
- PostgreSQL 13 or later
- Redis 6 or later
- uv (Python dependency manager)
- Sudo access for system packages
- At least 4GB RAM

## Step 1: Install System Dependencies

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install -y python3.12 python3.12-dev python3.12-venv \
    postgresql postgresql-contrib redis-server \
    build-essential libpq-dev git curl
```

### CentOS/RHEL

```bash
sudo dnf install -y python3.12 python3.12-devel \
    postgresql postgresql-server redis \
    gcc gcc-c++ make libpq-devel git
```

## Step 2: Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

## Step 3: Clone and Setup Project

```bash
git clone https://github.com/argonne-lcf/FIRST.git
cd inference-gateway

# Install dependencies
uv sync
```

## Step 4: Configure PostgreSQL

### Initialize PostgreSQL (if first time)

#### Ubuntu/Debian (usually auto-initialized)

```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### CentOS/RHEL

```bash
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### Create Database and User

Start a PostgreSQL shell:
```bash
sudo -u postgres psql
```

Create database and user
```bash
CREATE DATABASE inferencegateway;
CREATE USER inferencedev WITH PASSWORD 'your-secure-password';
ALTER ROLE inferencedev SET client_encoding TO 'utf8';
ALTER ROLE inferencedev SET default_transaction_isolation TO 'read committed';
ALTER ROLE inferencedev SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE inferencegateway TO inferencedev;
```

Exit shell
```bash
\q
```

### Configure PostgreSQL Authentication

Edit `/etc/postgresql/*/main/pg_hba.conf` (path may vary):

```
# Add this line (adjust for your security needs)
host    inferencegateway    inferencedev    127.0.0.1/32    md5
```

Restart PostgreSQL:

```bash
sudo systemctl restart postgresql
```

## Step 5: Configure Redis

Start and enable Redis:

```bash
sudo systemctl start redis
sudo systemctl enable redis
```

Verify Redis is running (should return `PONG`)
```bash
redis-cli ping
```

## Step 6: Configure Environment

Create a `.env` file from the [example environment file](https://github.com/argonne-lcf/FIRST/blob/main/env.example) and customize the `.env` file following the instructions found in the example file:
```bash
cp env.example .env
```

Make sure you include all of the Globus UUIDs and secrets generated during the [Globus setup](globus-setup.md) stage. You can generate the `SECRET_KEY` variable with the following Django command (if installed):
```bash
uv run -- python -c 'from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())'
```

!!! warning "Production Security"
    For production deployments:
    
    - Set `RUNNING_AUTOMATED_TEST_SUITE=False`
    - Set `DEBUG=False`
    - Use secure passwords and secrets
    - Add your domain to `ALLOWED_HOSTS` or use "*" if appropriate
    - Add at least one Globus High Assurance policy (`GLOBUS_POLICIES`)
    - Set authorized IDP domains (`AUTHORIZED_IDP_DOMAINS`) to match the policy

## Step 7: Initialize Database

Apply Django models to the database
```bash
uv run -- ./manage.py makemigrations
uv run -- ./manage.py migrate
```

## Step 8: Test the Gateway

Run development server:

```bash
uv run -- ./manage.py runserver
```

In another terminal, execute the following command:
```bash
curl http://localhost:8000/resource_server/whoami
```

If everything is running, the command should give you the following error:
```bash
Missing ('Authorization': 'Bearer <your-access-token>') in request headers.
```

## Step 9: Setup Production Server (Gunicorn)

### Install Gunicorn (already included in pyproject dependencies)

Create a systemd service file:

```bash
sudo nano /etc/systemd/system/inference-gateway.service
```

Add the following:

```ini
[Unit]
Description=FIRST Inference Gateway
After=network.target postgresql.service redis.service

[Service]
Type=notify
User=your-username
Group=your-username
WorkingDirectory=/path/to/inference-gateway
Environment="PATH=/path/to/inference-gateway/.venv/bin"
EnvironmentFile=/path/to/inference-gateway/.env
ExecStart=/path/to/inference-gateway/.venv/bin/gunicorn \
    inference_gateway.asgi:application \
    -k uvicorn.workers.UvicornWorker \
    -b 0.0.0.0:8000 \
    --workers 4 \
    --log-level info \
    --access-logfile /path/to/inference-gateway/logs/access.log \
    --error-logfile /path/to/inference-gateway/logs/error.log

[Install]
WantedBy=multi-user.target
```

### Start and Enable Service

```bash
# Create logs directory
mkdir -p logs

# Reload systemd
sudo systemctl daemon-reload

# Start service
sudo systemctl start inference-gateway

# Enable on boot
sudo systemctl enable inference-gateway
```

Verify that the service is running
```bash
sudo systemctl status inference-gateway
```

## [Optional] Step 10: Configure Nginx

Install Nginx:
```
# Ubuntu/Debian
sudo apt install nginx

# CentOS/RHEL
sudo dnf install nginx
```

Create site configuration:

```bash
sudo nano /etc/nginx/sites-available/inference-gateway
```

Add the following:

```nginx
upstream inference_gateway {
    server 127.0.0.1:8000 fail_timeout=0;
}

server {
    listen 80;
    server_name your-domain.com;
    client_max_body_size 100M;

    location / {
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Host $http_host;
        proxy_redirect off;
        proxy_buffering off;
        proxy_pass http://inference_gateway;
    }
}
```

Enable the site:

```bash
# Ubuntu/Debian
sudo ln -s /etc/nginx/sites-available/inference-gateway /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx

# CentOS/RHEL
sudo ln -s /etc/nginx/sites-available/inference-gateway /etc/nginx/conf.d/
sudo nginx -t
sudo systemctl restart nginx
```

### Setup SSL with Let's Encrypt

```bash
# Ubuntu/Debian
sudo apt install certbot python3-certbot-nginx

# CentOS/RHEL
sudo dnf install certbot python3-certbot-nginx

# Get certificate
sudo certbot --nginx -d your-domain.com
```

## Step 11: Configure Firewall

```bash
# Ubuntu/Debian (UFW)
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable

# CentOS/RHEL (firewalld)
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload
```

## Maintenance

### View Logs

```bash
# Application logs
tail -f logs/error.log
tail -f logs/access.log

# System service logs
sudo journalctl -u inference-gateway -f
```

### Restart Service

```bash
sudo systemctl restart inference-gateway
```

### Update Application

```bash
cd /path/to/inference-gateway
git pull origin main
uv sync
uv run -- ./manage.py migrate
sudo systemctl restart inference-gateway
```

## Troubleshooting

### Service won't start

Check logs:

```bash
sudo journalctl -u inference-gateway -n 50
```

Check configuration:

```bash
uv run -- ./manage.py check
```

### Database connection errors

Verify PostgreSQL is running:

```bash
sudo systemctl status postgresql
```

Test connection:

```bash
psql -h localhost -U inferencedev -d inferencegateway
```

### Permission errors

Ensure the service user owns the files:

```bash
sudo chown -R your-username:your-username /path/to/inference-gateway
```

### Nginx errors

Check nginx error log:

```bash
sudo tail -f /var/log/nginx/error.log
```

Test configuration:

```bash
sudo nginx -t
```

## Next Steps

- [Configure Inference Backends](../inference-setup/index.md)
- [Production Best Practices](../deployment/production.md)
- [Monitoring Setup](../monitoring.md)

## Additional Resources

- [Configuration Reference](configuration.md)
- [Gunicorn Documentation](https://docs.gunicorn.org/)
- [Nginx Documentation](https://nginx.org/en/docs/)

