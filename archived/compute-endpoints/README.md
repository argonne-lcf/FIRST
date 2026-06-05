# Globus Compute Endpoint Configurations

This directory contains production-ready endpoint configurations and helper scripts for deploying vLLM inference servers with Globus Compute on HPC clusters.

## Overview

These examples are based on our production deployment at **Argonne Leadership Computing Facility (ALCF) Sophia cluster**. They demonstrate best practices for:

- Multi-node model serving with Ray
- Dynamic environment management
- Production-grade logging and error handling
- Advanced vLLM configurations

**Important**: These configurations are **cluster-specific** and must be adapted to your HPC environment. See [Adapting for Your Cluster](#adapting-for-your-cluster) below.

## Files

### Helper Scripts

#### `launch_vllm_model.sh`
Modular vLLM launcher script with comprehensive features:

- **Single and multi-node support**: Automatically detects and configures Ray for pipeline parallelism
- **Dynamic vLLM configuration**: Command-line arguments for all major vLLM parameters
- **Health monitoring**: Startup verification with timeout and retry logic
- **Flexible deployment**: Works with PBS, Slurm, or local execution

**Usage:**
```bash
source launch_vllm_model.sh \
  --model-name meta-llama/Meta-Llama-3.1-8B-Instruct \
  --vllm-version v0.11.0 \
  --tensor-parallel 8 \
  --max-model-len 8192 \
  --enable-chunked-prefill \
  --enable-prefix-caching \
  --gpu-memory-util 0.95
```

Run with `--help` for full documentation.

#### `sophia_env_setup_with_ray.sh`
Environment setup script for ALCF Sophia cluster:

- **Dynamic version selection**: Automatically loads correct conda environment based on `VLLM_VERSION`
- **Module management**: ALCF-specific modules (conda, gcc, spack-pe-base)
- **Ray cluster management**: Automated multi-node Ray setup using PBS nodefile
- **Comprehensive utilities**: Cleanup, monitoring, and troubleshooting functions

**Key Functions:**
- `setup_environment()` - Initialize conda environment and exports
- `setup_ray_cluster()` - Setup multi-node Ray cluster for large models
- `start_model()` - Start vLLM with retry logic and health checks
- `cleanup_python_processes()` - Clean up zombie processes
- `stop_ray()` - Gracefully stop Ray cluster

**Usage:**
```bash
source sophia_env_setup_with_ray.sh
setup_environment
setup_ray_cluster  # For multi-node only
```

### Endpoint Configuration Examples

#### Single-Node Configurations

**`local-vllm-endpoint.yaml`**
- Basic local/workstation deployment
- Uses `LocalProvider` (no job scheduler)
- Includes inline environment setup and retry logic
- Good starting point for development

**`sophia-vllm-singlenode-example.yaml`**
- Production single-node deployment on ALCF Sophia
- 1 node, 8 GPUs (tensor parallelism)
- Uses `launch_vllm_model.sh` for robust deployment
- PBS scheduling with optimized settings

#### Multi-Node Configurations

**`sophia-vllm-multinode-example.yaml`**
- Production multi-node deployment for large models (70B-405B)
- 4 nodes, 32 GPUs (TP=8, PP=4)
- Automatic Ray cluster setup via `launch_vllm_model.sh`
- PBS scheduling with multi-node allocation

**`sophia-vllm-toolcalling-example.yaml`**
- Single-node deployment with tool calling support
- Custom chat templates for function calling
- Llama 4 models with pythonic tool parser

#### Specialized Configurations

**`sophia-vllm-batch-template.yaml`**
- Batch processing endpoint
- Lower idle timeout for ephemeral jobs
- Minimal blocks for cost efficiency

**`pbs-qstat-example.yaml`**
- Job scheduler monitoring endpoint
- Runs on login node (no GPU)
- Provides cluster status information to gateway

## Architecture

### Production Setup Flow

```
┌─────────────────────────────────────────────────────────┐
│ Globus Compute Endpoint Configuration (YAML)           │
│                                                         │
│  worker_init: |                                         │
│    source launch_vllm_model.sh \                        │
│      --model-name ... \                                 │
│      --vllm-version v0.11.0                             │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│ launch_vllm_model.sh                                    │
│                                                         │
│  1. Sources sophia_env_setup_with_ray.sh                │
│  2. Calls setup_environment()                           │
│  3. Detects single vs multi-node mode                   │
│  4. Sets up Ray cluster if needed                       │
│  5. Builds vLLM command from arguments                  │
│  6. Calls start_model() with retry logic                │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│ sophia_env_setup_with_ray.sh                            │
│                                                         │
│  - Loads modules (conda, gcc, spack)                    │
│  - Activates correct conda environment                  │
│  - Sets HF cache, proxy, NCCL settings                  │
│  - Manages Ray cluster (if multi-node)                  │
│  - Monitors model startup                               │
└─────────────────────────────────────────────────────────┘
```

### Multi-Node Ray Setup

For large models requiring pipeline parallelism:

1. `PBS_NODEFILE` parsed to identify head and worker nodes
2. Ray head started on first node
3. Ray workers started on remaining nodes
4. vLLM launched with Ray backend and appropriate TP/PP settings

## Adapting for Your Cluster

### 1. Environment Setup Script

Create your own based on `sophia_env_setup_with_ray.sh`:

**Required Changes:**
```bash
# Proxy settings (or remove if not needed)
export HTTP_PROXY="your-proxy:port"

# Module system
module use /your/module/path
module load your-conda-module

# Conda environments
CONDA_ENV="/your/path/to/vllm-v0.11.0-env/"

# HuggingFace cache
export HF_HOME='/your/model/cache/'

# Network interface (run 'ifconfig' on compute node)
export NCCL_SOCKET_IFNAME='your-interface'  # e.g., 'ib0', 'eth0', 'ens0'

# Node resources
export RAY_NUM_CPUS=64   # Adjust for your nodes
export RAY_NUM_GPUS=8    # Adjust for your nodes
```

### 2. Launcher Script (Optional)

`launch_vllm_model.sh` is fairly generic. You may need to:

- Update default environment setup script path (line 242-246)
- Adjust SSL certificate paths
- Modify default parameter values

### 3. Endpoint YAML Configuration

**For PBS:**
```yaml
provider:
  type: PBSProProvider
  account: your_project
  queue: your_queue
  scheduler_options: |
    #PBS -l your:cluster:options
```

**For Slurm:**
```yaml
provider:
  type: SlurmProvider
  account: your_account
  partition: your_partition
  scheduler_options: |
    #SBATCH --your-options
```

### 4. Cluster-Specific Checklist

| Item | Check Method | Example |
|------|--------------|---------|
| **Network Interface** | `ifconfig` on compute node | `infinibond0`, `ib0`, `eth0` |
| **Module System** | `module avail` | Path to conda module |
| **File System** | Shared storage path | `/scratch`, `/home`, `/gpfs` |
| **Scheduler** | PBS or Slurm | Queue names, account codes |
| **GPU Allocation** | Scheduler syntax | `ngpus=8`, `--gpus-per-node=8` |
| **Proxy** | Required for internet | `http://proxy:3128` or none |
| **SSL Certs** | For HTTPS vLLM | Path or disable |

## Quick Start

### 1. Copy and Customize Scripts

```bash
# Copy environment setup script
cp sophia_env_setup_with_ray.sh your_cluster_env_setup.sh

# Edit with your cluster-specific settings
vim your_cluster_env_setup.sh
```

### 2. Update Launcher Script Reference

```bash
# Edit launch_vllm_model.sh line 242
ENV_SETUP_SCRIPT="${ENV_SETUP_SCRIPT:-/path/to/your_cluster_env_setup.sh}"
```

### 3. Create Endpoint Configuration

```bash
# Start with single-node example
cp sophia-vllm-singlenode-example.yaml my-cluster-endpoint.yaml

# Edit with your settings
vim my-cluster-endpoint.yaml
```

### 4. Configure and Start Endpoint

```bash
# Configure endpoint
globus-compute-endpoint configure my-endpoint

# Copy your YAML to the config directory
cp my-cluster-endpoint.yaml ~/.globus_compute/my-endpoint/config.yaml

# Start endpoint
globus-compute-endpoint start my-endpoint
```

## Troubleshooting

### Common Issues

**Module not found:**
```bash
module avail  # Check available modules
module use /correct/path  # Add correct module path
```

**Conda activation fails:**
```bash
conda init bash
source ~/.bashrc
conda env list  # Verify environment exists
```

**NCCL errors (multi-GPU):**
```bash
ifconfig  # Find correct network interface
export NCCL_SOCKET_IFNAME='correct-interface'
```

**Ray cluster issues:**
```bash
cat $PBS_NODEFILE  # Verify nodes allocated
mpiexec -n 2 hostname  # Test node communication
ray status  # Check Ray cluster state
```

**vLLM startup timeout:**
```bash
# Check logs in endpoint directory
tail -f ~/.globus_compute/my-endpoint/endpoint.log

# Check vLLM logs
tail -f vllm.log

# Check PBS/Slurm job logs
qstat -f <job-id>  # PBS
squeue -j <job-id>  # Slurm
```

## Best Practices

1. **Test locally first**: Use `local-vllm-endpoint.yaml` as a starting point
2. **Start small**: Deploy a small model (e.g., `facebook/opt-125m`) for testing
3. **Check logs**: Always review `endpoint.log` and `vllm.log` for errors
4. **Monitor resources**: Use `qstat`/`squeue` to verify job allocation
5. **Keep nodes warm**: Set `min_blocks > 0` for production to reduce cold start latency
6. **Use versioned environments**: Create separate conda envs for each vLLM version

## Additional Resources

- **Documentation**: [Globus Compute Setup Guide](../docs/admin-guide/inference-setup/globus-compute.md)
- **Compute Functions**: See `../compute-functions/` for function registration scripts
- **Gateway Configuration**: See `../fixtures/` for endpoint registration examples

## Support

For ALCF-specific issues, contact ALCF Support.

For FIRST Gateway issues, open an issue on [GitHub](https://github.com/auroraGPT-ANL/inference-gateway/issues).

For Globus Compute issues, see [Globus Compute Documentation](https://globus-compute.readthedocs.io/).

---

**Note**: These configurations represent a production deployment at a specific HPC facility. Your mileage may vary depending on your cluster's architecture, scheduler, and policies. Always test thoroughly before production deployment.

