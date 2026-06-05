# Kubernetes Deployment

!!! warning "Coming Soon"
    Kubernetes deployment manifests and Helm charts are currently under development.

## Planned Features

The Kubernetes deployment will include:

- **Helm Chart**: Easy installation and configuration management
- **High Availability**: Multi-replica deployments with load balancing
- **Auto-Scaling**: Horizontal Pod Autoscaler based on metrics
- **StatefulSets**: For PostgreSQL and Redis persistence
- **Ingress Configuration**: HTTPS/TLS termination and routing
- **Secrets Management**: Kubernetes secrets for sensitive data
- **ConfigMaps**: Environment-specific configuration
- **Health Probes**: Liveness and readiness checks
- **Resource Limits**: CPU and memory management
- **Monitoring Integration**: Prometheus and Grafana

## Current Status

We are actively working on:

1. Creating Kubernetes manifests for all components
2. Developing a Helm chart for simplified deployment
3. Testing on various Kubernetes distributions (EKS, GKE, OpenShift)
4. Documentation and best practices

## Alternative: Docker Deployment

For now, please use one of these deployment methods:

- [Docker Deployment](../gateway-setup/docker.md) - Containerized deployment with Docker Compose
- [Bare Metal Setup](../gateway-setup/bare-metal.md) - Direct installation on servers

## Get Notified

To be notified when Kubernetes support is available:

- :star: Star the [GitHub repository](https://github.com/auroraGPT-ANL/inference-gateway)
- Watch the repository for releases
- Check the [releases page](https://github.com/auroraGPT-ANL/inference-gateway/releases)

## Contribute

Interested in helping with Kubernetes deployment?

- Check open issues tagged with `kubernetes`
- Submit a pull request
- Share your deployment configurations

## Contact

For enterprise Kubernetes deployments or consulting:

- Open an issue on GitHub
- Contact the development team

---

**Last Updated**: November 2025

Check back soon for updates!

