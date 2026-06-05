# FIRST

Welcome to the documentation for the **Federated Inference Resource Scheduling Toolkit (FIRST)**. FIRST enables AI Model inference as a service across distributed HPC clusters through an OpenAI-compatible API.

## What is FIRST?

FIRST (Federated Inference Resource Scheduling Toolkit) is a system that allows secure, remote execution of Inference on AI Models through an OpenAI-compatible API. It validates and authorizes inference requests to scientific computing clusters using Globus Auth and Globus Compute.

## System Architecture

![System Architecture](first_architecture.png)

The Inference Gateway consists of several components:

- **API Gateway**: Django-based REST/Ninja API that handles authorization and request routing
- **Globus Auth**: Authentication and authorization service
- **Globus Compute Endpoints**: Remote execution framework on HPC clusters (or local machines)
- **Inference Server Backend**: High-performance inference service for LLMs (e.g., vLLM)

## Quick Links

### For Administrators

- **[Globus Setup](admin-guide/gateway-setup/globus-setup.md)** - Create Globus project and register applications
- **[Docker Deployment](admin-guide/gateway-setup/docker.md)** - Fast-track Docker deployment in under 10 minutes
- **[Bare Metal Setup](admin-guide/gateway-setup/bare-metal.md)** - Complete installation on your own infrastructure
- **[Inference Backend](admin-guide/inference-setup/index.md)** - Connect to OpenAI API, local vLLM, or Globus Compute
- **[Kubernetes](admin-guide/deployment/kubernetes.md)** - Deploy on Kubernetes clusters (Coming Soon)

### For Users

- **[User Guide](user-guide/index.md)** - Complete guide for authentication and making requests
- **[API Reference](reference/api.md)** - API endpoint documentation
- **[Examples](user-guide/index.md#using-the-openai-python-sdk)** - Code examples and tutorials

## Key Features

- **Federated Access**: Route requests across multiple HPC clusters automatically
- **OpenAI-Compatible**: Works with existing OpenAI SDK and tools
- **Secure**: Globus Auth integration with group-based access control
- **High Performance**: Support for vLLM and other optimized inference backends
- **Flexible**: Deploy via Docker, bare metal, or Kubernetes
- **Scalable**: Auto-scaling and resource management for HPC environments

## Example Deployment

For a production example, see the [ALCF Inference Endpoints](https://github.com/argonne-lcf/inference-endpoints) documentation.

## Getting Help

- **GitHub**: [Report issues or contribute](https://github.com/auroraGPT-ANL/inference-gateway)
- **Citation**: [Research Paper](reference/citation.md)
- **API Reference**: [Complete API documentation](reference/api.md)

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/auroraGPT-ANL/inference-gateway/blob/main/LICENSE) file for details.

---

!!! tip "Quick Start Paths"
    - **Just want to try it out?** → [Docker Quickstart](admin-guide/gateway-setup/docker.md)
    - **Need full control?** → [Bare Metal Setup](admin-guide/gateway-setup/bare-metal.md)
    - **Want to use the API?** → [User Guide](user-guide/index.md)

