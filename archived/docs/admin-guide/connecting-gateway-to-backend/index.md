# Connecting Gateway to Backends with Adaptors

This section will guide you through connecting different types of inference backends to the Gateway. The implementation is based on `adaptors`, which provide the common functionalities needed to integrate with the Gateway API codes. FIRST provides adaptors for Globus Compute and direct API connections that can be used out-of-the-box. However, custom adaptors can also be built to integrate with arbitrary backends or to refine/adapt existing ones.

Choose your adaptors and follow instructions:

- [Globus Compute Adaptors](globus-compute.md)
- [Direct API Adaptors](direct-api.md)
- [Custom Adaptors](custom.md)

Once you have integrated your endpoint(s) and cluster(s) in the fixtures, incorporate them into the database:

```bash
# Docker
docker-compose exec inference-gateway python manage.py loaddata fixtures/endpoints.json
docker-compose exec inference-gateway python manage.py loaddata fixtures/clusters.json

# Bare Metal
python manage.py loaddata fixtures/endpoints.json
python manage.py loaddata fixtures/clusters.json
```

