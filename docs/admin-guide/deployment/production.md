# Production Best Practices

This guide covers best practices for deploying FIRST Inference Gateway in production environments.

## Security

### Authentication & Authorization

Restrict access to:
 - specific identity providers (`AUTHORIZED_IDP_DOMAINS` and Globus High-Assurance policy)
 - specific groups (`GLOBUS_GROUPS` and `AUTHORIZED_GROUPS_PER_IDP`)

See [example environment file](https://github.com/argonne-lcf/FIRST/blob/main/env.example) and [Globus Setup](../gateway-setup/globus-setup.md) for more details.

### Secrets Management

**Never** store secrets in code or version control.

#### Use Environment Files

```bash
# .env (add to .gitignore)
SECRET_KEY="..."
POSTGRES_PASSWORD="..."
```

#### Docker Secrets

```yaml
services:
  gateway:
    secrets:
      - db_password
      - globus_secret

secrets:
  db_password:
    file: ./secrets/db_password.txt
  globus_secret:
    file: ./secrets/globus_secret.txt
```

#### Vault Integration

For enterprise deployments, integrate with HashiCorp Vault or similar.

### HTTPS/TLS

Always use HTTPS in production.

#### Let's Encrypt with Certbot

```bash
sudo certbot --nginx -d yourdomain.com
```

#### Custom Certificates

```nginx
server {
    listen 443 ssl http2;
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
}
```

### Firewall Configuration

```bash
# Ubuntu/Debian
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw deny 8000/tcp  # Don't expose Django directly

# CentOS/RHEL
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload
```

## Performance

### Database Optimization

#### Connection Pooling

```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'CONN_MAX_AGE': 600,  # Persistent connections
        'OPTIONS': {
            'connect_timeout': 10,
        }
    }
}
```

#### Indexes

Ensure proper indexes on frequently queried fields:

```python
python manage.py dbshell
CREATE INDEX idx_endpoint_slug ON resource_server_endpoint(endpoint_slug);
CREATE INDEX idx_created_at ON resource_server_listendpointslog(created_at);
```

### Caching

#### Redis Configuration

```python
# settings.py
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': 'redis://redis:6379/0',
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'CONNECTION_POOL_KWARGS': {
                'max_connections': 50
            }
        }
    }
}
```

### Gunicorn Configuration

#### Worker Calculation

```python
workers = (2 * CPU_cores) + 1
```

For a 16-core machine:

```python
workers = (2 * 16) + 1 = 33
```

#### Production Config

```python
# gunicorn_asgi.config.py
import multiprocessing

bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
timeout = 120
keepalive = 5
max_requests = 1000
max_requests_jitter = 50
```

### Nginx Optimization

```nginx
upstream gateway {
    least_conn;  # Load balancing algorithm
    server 127.0.0.1:8000 max_fails=3 fail_timeout=30s;
    server 127.0.0.1:8001 max_fails=3 fail_timeout=30s;
    keepalive 64;
}

server {
    listen 443 ssl http2;
    
    # Gzip compression
    gzip on;
    gzip_types text/plain text/css application/json application/javascript;
    gzip_min_length 1000;
    
    # Client body size
    client_max_body_size 100M;
    client_body_buffer_size 1M;
    
    # Timeouts
    proxy_connect_timeout 600s;
    proxy_send_timeout 600s;
    proxy_read_timeout 600s;
    send_timeout 600s;
    
    # Buffering
    proxy_buffering off;  # Important for streaming
    proxy_request_buffering off;
    
    # Headers
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Host $http_host;
    proxy_set_header X-Real-IP $remote_addr;
    
    location /static/ {
        alias /path/to/staticfiles/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }
    
    location / {
        proxy_pass http://gateway;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

## Monitoring

### Application Monitoring

#### Prometheus Metrics

Add to `docker-compose.yml`:

```yaml
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
```

#### Grafana Dashboards

```yaml
services:
  grafana:
    image: grafana/grafana
    volumes:
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=secure_password
```

### Log Aggregation

#### Structured Logging

```python
# logging_config.py
LOGGING = {
    'version': 1,
    'formatters': {
        'json': {
            'class': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(asctime)s %(name)s %(levelname)s %(message)s'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/gateway.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 10,
            'formatter': 'json'
        }
    }
}
```

#### ELK Stack Integration

For large deployments, consider Elasticsearch + Logstash + Kibana.

### Health Checks

#### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 5
```

#### Custom Health Endpoint

Create a health check view in Django to verify database, Redis, and Globus Compute connectivity.

## Backup & Recovery

### Database Backups

#### Automated Backups

```bash
#!/bin/bash
# backup_db.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/postgres"
BACKUP_FILE="$BACKUP_DIR/backup_$DATE.sql.gz"

pg_dump -h localhost -U inferencedev inferencegateway | gzip > $BACKUP_FILE

# Keep only last 30 days
find $BACKUP_DIR -name "backup_*.sql.gz" -mtime +30 -delete
```

Add to crontab:

```bash
0 2 * * * /path/to/backup_db.sh
```

#### Point-in-Time Recovery

Configure PostgreSQL for WAL archiving:

```ini
# postgresql.conf
wal_level = replica
archive_mode = on
archive_command = 'cp %p /backup/wal/%f'
```

### Configuration Backups

```bash
# Backup environment and fixtures
tar -czf config_backup_$(date +%Y%m%d).tar.gz \
    .env \
    fixtures/ \
    nginx_app.conf \
    gunicorn_asgi.config.py
```

## Scaling

### Horizontal Scaling

#### Multiple Gateway Instances

```nginx
upstream gateway {
    server gateway1:8000;
    server gateway2:8000;
    server gateway3:8000;
}
```

#### Session Affinity

For stateful sessions:

```nginx
upstream gateway {
    ip_hash;
    server gateway1:8000;
    server gateway2:8000;
}
```

### Database Scaling

#### Read Replicas

```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'HOST': 'primary.db.internal',
    },
    'replica': {
        'ENGINE': 'django.db.backends.postgresql',
        'HOST': 'replica.db.internal',
    }
}

DATABASE_ROUTERS = ['path.to.ReplicaRouter']
```

#### Connection Pooling (PgBouncer)

```ini
# pgbouncer.ini
[databases]
inferencegateway = host=localhost port=5432 dbname=inferencegateway

[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 20
```

### Inference Backend Scaling

#### Federated Endpoints

Deploy multiple Globus Compute endpoints and configure federated routing for automatic load balancing.

#### Auto-Scaling

Configure Globus Compute endpoints to auto-scale based on demand:

```yaml
engine:
  provider:
    min_blocks: 1
    max_blocks: 20
```

## Maintenance

### Zero-Downtime Deployments

#### Blue-Green Deployment

1. Deploy new version alongside old
2. Switch traffic to new version
3. Monitor for issues
4. Decommission old version

#### Rolling Updates

```bash
# Update one instance at a time
for server in gateway1 gateway2 gateway3; do
    ssh $server "cd /app && git pull && systemctl restart gateway"
    sleep 60  # Allow time to stabilize
done
```

### Database Migrations

Always test migrations in staging first:

```bash
# Backup before migrating
./backup_db.sh

# Run migration
python manage.py migrate

# If issues occur, restore backup
psql -h localhost -U inferencedev inferencegateway < backup.sql
```

## Disaster Recovery

### Disaster Recovery Plan

1. **Recovery Time Objective (RTO)**: 2 hours
2. **Recovery Point Objective (RPO)**: 1 hour

### Backup Strategy

- **Hourly**: Database transaction logs
- **Daily**: Full database backup
- **Weekly**: Complete system backup (config, logs, data)
- **Monthly**: Archived to off-site storage

### Failover Procedures

Document step-by-step procedures for:

1. Gateway failure → Switch to backup gateway
2. Database failure → Promote read replica
3. Complete site failure → Activate DR site

## Checklist

### Pre-Production

- [ ] All secrets are externalized
- [ ] HTTPS/TLS configured
- [ ] Firewall rules applied
- [ ] DEBUG=False
- [ ] Strong passwords set
- [ ] Database backed up
- [ ] Monitoring configured
- [ ] Log aggregation set up
- [ ] Health checks working
- [ ] Load testing completed
- [ ] Disaster recovery plan documented

### Post-Deployment

- [ ] Monitor logs for errors
- [ ] Verify all endpoints responding
- [ ] Check database performance
- [ ] Test authentication flow
- [ ] Verify Globus Compute connectivity
- [ ] Run integration tests
- [ ] Document any issues

## Additional Resources

- [Django Security Best Practices](https://docs.djangoproject.com/en/stable/topics/security/)
- [Nginx Performance Tuning](https://www.nginx.com/blog/tuning-nginx/)
- [PostgreSQL Performance Tips](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [Monitoring Guide](../monitoring.md)

