# Monitoring & Troubleshooting

This guide covers monitoring the FIRST Inference Gateway and troubleshooting common issues.

## Monitoring

### Application Logs

#### Docker Deployment

```bash
# View all logs
docker-compose logs -f

# View gateway logs only
docker-compose logs -f inference-gateway

# Last 100 lines
docker-compose logs --tail=100 inference-gateway
```

#### Bare Metal Deployment

```bash
# Application logs
tail -f logs/django_info.log

# Gunicorn logs
tail -f logs/backend_gateway.error.log
tail -f logs/backend_gateway.access.log

# Systemd service logs
sudo journalctl -u inference-gateway -f
```

### Database Monitoring

```bash
# Connection stats
psql -h localhost -U inferencedev -d inferencegateway -c "SELECT * FROM pg_stat_activity;"

# Table sizes
psql -h localhost -U inferencedev -d inferencegateway -c "
SELECT schemaname,tablename,pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables WHERE schemaname='public' ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
```

### Redis Monitoring

```bash
# Connect to Redis CLI
redis-cli

# In Redis CLI:
INFO
DBSIZE
MONITOR  # Watch commands in real-time
```

### Globus Compute Endpoints

```bash
# List endpoints
globus-compute-endpoint list

# Check status
globus-compute-endpoint status my-endpoint

# View logs
globus-compute-endpoint log my-endpoint -n 100

# Follow logs
tail -f ~/.globus_compute/my-endpoint/endpoint.log
```

## Common Issues

### Gateway Won't Start

**Symptoms**: Container/service fails to start

**Check**:

```bash
# Docker
docker-compose logs inference-gateway

# Bare metal
sudo journalctl -u inference-gateway -n 50
python manage.py check
```

**Common Causes**:

- Missing environment variables
- Database connection failure
- Port already in use
- Syntax error in settings

### Database Connection Errors

**Symptoms**: `OperationalError: could not connect to server`

**Solutions**:

```bash
# Verify PostgreSQL is running
sudo systemctl status postgresql
docker-compose ps postgres

# Test connection
psql -h localhost -U inferencedev -d inferencegateway

# Check pg_hba.conf
sudo nano /etc/postgresql/*/main/pg_hba.conf

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Authentication Failures

**Symptoms**: 401 Unauthorized, Globus token errors

**Solutions**:

1. Verify Globus application credentials in `.env`
2. Check scope was created successfully:
   ```bash
   curl -s --user $CLIENT_ID:$CLIENT_SECRET \
       https://auth.globus.org/v2/api/clients/$CLIENT_ID
   ```
3. Force re-authentication:
   ```bash
   python inference-auth-token.py authenticate --force
   ```
4. Verify redirect URIs match in Globus app settings

### Globus Compute Errors

**Symptoms**: Function execution failures, timeout errors

**Solutions**:

```bash
# Check endpoint is running
globus-compute-endpoint list

# Restart endpoint
globus-compute-endpoint restart my-endpoint

# View detailed logs
globus-compute-endpoint log my-endpoint -n 200

# Verify function UUID is allowed
cat ~/.globus_compute/my-endpoint/config.yaml
```

### Model Not Found

**Symptoms**: `Model 'xxx' not found` errors

**Solutions**:

1. Verify fixture was loaded:
   ```bash
   python manage.py dumpdata resource_server.endpoint
   ```
2. Check model name matches exactly in fixture
3. Reload fixtures:
   ```bash
   python manage.py loaddata fixtures/endpoints.json
   ```

### Slow Response Times

**Causes**:

- Cold start (first request to endpoint)
- GPU not available
- Model loading time
- Network latency

**Solutions**:

1. Enable hot nodes (min_blocks > 0 in Globus Compute config)
2. Monitor GPU usage: `nvidia-smi`
3. Check vLLM logs for bottlenecks
4. Increase Gunicorn timeout:
   ```python
   timeout = 300
   ```

### Out of Memory Errors

**Symptoms**: OOM kills, CUDA out of memory

**Solutions**:

```bash
# vLLM: Reduce GPU memory usage
--gpu-memory-utilization 0.7

# vLLM: Use quantization
--quantization awq

# vLLM: Reduce context length
--max-model-len 2048

# System: Add swap space
sudo fallocate -l 32G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

## Health Checks

### Manual Health Checks

```bash
# Gateway health
curl http://localhost:8000/

# vLLM health
curl http://localhost:8001/health

# Database connectivity
python manage.py dbshell

# Redis connectivity
redis-cli ping
```

### Automated Health Monitoring

Create a health check script:

```bash
#!/bin/bash
# health_check.sh

# Check gateway
if curl -s http://localhost:8000/ > /dev/null; then
    echo "✓ Gateway is healthy"
else
    echo "✗ Gateway is down"
    systemctl restart inference-gateway
fi

# Check database
if psql -h localhost -U inferencedev -d inferencegateway -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✓ Database is healthy"
else
    echo "✗ Database is down"
fi
```

Add to crontab:

```bash
*/5 * * * * /path/to/health_check.sh >> /var/log/health_check.log 2>&1
```

## Performance Metrics

### Key Metrics to Monitor

1. **Request Rate**: Requests per second
2. **Latency**: Response time (p50, p95, p99)
3. **Error Rate**: Percentage of failed requests
4. **Queue Depth**: Pending Globus Compute tasks
5. **GPU Utilization**: GPU memory and compute usage
6. **Database Connections**: Active connections
7. **Cache Hit Rate**: Redis cache effectiveness

### Prometheus Metrics

If using Prometheus, key metrics to track:

```
# Request metrics
http_requests_total
http_request_duration_seconds

# Globus Compute metrics
globus_compute_tasks_submitted
globus_compute_tasks_completed
globus_compute_tasks_failed

# System metrics
process_cpu_seconds_total
process_resident_memory_bytes
```

## Troubleshooting Checklist

When issues occur, work through this checklist:

- [ ] Check application logs
- [ ] Verify all services are running
- [ ] Test database connectivity
- [ ] Check Redis connectivity
- [ ] Verify Globus Compute endpoints are online
- [ ] Test authentication flow
- [ ] Check network connectivity
- [ ] Review recent configuration changes
- [ ] Check disk space
- [ ] Monitor resource usage (CPU, RAM, GPU)

## Getting Help

If you're still stuck:

1. **Check documentation**: Review the relevant setup guides
2. **Search issues**: Look for similar issues on [GitHub](https://github.com/auroraGPT-ANL/inference-gateway/issues)
3. **Enable debug logging**: Set `DEBUG=True` temporarily
4. **Collect information**:
   - Version information
   - Error messages and stack traces
   - Configuration (sanitize secrets!)
   - Relevant log excerpts
5. **Open an issue**: Provide all collected information

## Additional Resources

- [Production Best Practices](deployment/production.md)
- [Configuration Reference](gateway-setup/configuration.md)
- [Globus Compute Documentation](https://globus-compute.readthedocs.io/)

