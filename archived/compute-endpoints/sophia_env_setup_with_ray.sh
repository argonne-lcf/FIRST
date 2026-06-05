#!/bin/bash
################################################################################
# Sophia Environment Setup Script with Ray Support
# Version: 2.0
# Description: Environment setup and helper functions for vLLM on Sophia
################################################################################

################################
# Environment Setup Function   #
################################
setup_environment() {
    echo "=========================================="
    echo "Setting up Sophia environment..."
    echo "=========================================="

    # Proxy configuration for ALCF
    export HTTP_PROXY="http://proxy.alcf.anl.gov:3128"
    export HTTPS_PROXY="http://proxy.alcf.anl.gov:3128"
    export http_proxy=$HTTP_PROXY
    export https_proxy=$HTTPS_PROXY
    export ftp_proxy=$HTTP_PROXY

    # Load required modules
    echo "Loading modules..."
    module use /soft/modulefiles
    module load conda
    module load spack-pe-base
    module load gcc

    # Determine conda environment based on VLLM_VERSION
    if [ -z "$VLLM_VERSION" ]; then
        VLLM_VERSION="v0.11.0"
        echo "VLLM_VERSION not set, using default: $VLLM_VERSION"
    fi

    # Map version to conda environment
    case "$VLLM_VERSION" in
        v0.8.2)
            CONDA_ENV="/eagle/argonne_tpc/inference-gateway/envs/vllmv0.8.2/"
            ;;
        v0.8.5*)
            CONDA_ENV="/eagle/argonne_tpc/inference-gateway/envs/vllmv0.8.5.post1/"
            ;;
        v0.10.1)
            CONDA_ENV="/eagle/argonne_tpc/inference-gateway/envs/vllmv0.10.1/"
            ;;
        v0.11.0)
            CONDA_ENV="/eagle/argonne_tpc/inference-gateway/envs/vllmv0.11.0/"
            ;;
        *)
            echo "WARNING: Unknown VLLM_VERSION '$VLLM_VERSION', defaulting to v0.11.0"
            CONDA_ENV="/eagle/argonne_tpc/inference-gateway/envs/vllmv0.11.0/"
            ;;
    esac

    echo "Activating conda environment: $CONDA_ENV"
    conda activate "$CONDA_ENV"

    # Verify activation
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to activate conda environment: $CONDA_ENV"
        return 1
    fi

    echo "Active conda environment: $CONDA_DEFAULT_ENV"

    # HuggingFace cache and token configuration
    export HF_DATASETS_CACHE='/eagle/argonne_tpc/model_weights/'
    export HF_HOME='/eagle/argonne_tpc/model_weights/'
    export HF_HUB_CACHE='/eagle/argonne_tpc/model_weights/hub'
    export TRANSFORMERS_OFFLINE=1
    export HF_TOKEN=${HF_TOKEN} # Replace with your actual HuggingFace token

    # Ray configuration
    export RAY_TMPDIR='/tmp'

    # NCCL configuration for multi-GPU/multi-node
    export NCCL_SOCKET_IFNAME='infinibond0'

    # Threading configuration
    export OMP_NUM_THREADS=4

    # vLLM-specific settings
    export VLLM_LOG_LEVEL=WARN
    export USE_FASTSAFETENSOR=true
    export VLLM_IMAGE_FETCH_TIMEOUT=60
    export VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE=shm

    # Increase core dump size for debugging
    ulimit -c unlimited

    # Internal secrets for streaming
    export INTERNAL_STREAMING_SECRET=${INTERNAL_STREAMING_SECRET} # Replace with your actual internal streaming secret

    # Export path to this script for nested calls
    export COMMON_SETUP_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"

    echo "Environment setup complete."
    echo "  - Python: $(which python)"
    echo "  - vLLM version: $VLLM_VERSION"
    echo "  - Conda env: $CONDA_DEFAULT_ENV"
    echo "=========================================="

    return 0
}

################################
# Cleanup Python Processes     #
################################
cleanup_python_processes() {
    echo "Cleaning up existing Python processes..."

    # Define patterns to kill
    local patterns=("vllm serve" "vllm.entrypoints" "multiprocessing.spawn" "multiprocessing.resource_tracker" "ray::")

    for pattern in "${patterns[@]}"; do
        pids=$(pgrep -f "$pattern" 2>/dev/null)
        if [ -n "$pids" ]; then
            for pid in $pids; do
                echo "  Killing process $pid (pattern: $pattern)"
                kill -9 "$pid" 2>/dev/null || true
            done
        fi
    done

    # Give processes time to terminate
    sleep 2

    echo "Cleanup complete."
}

################################
# Ray Cluster Management       #
################################

# Function to stop Ray
stop_ray() {
    echo "Stopping Ray on $(hostname)..."
    ray stop -f 2>/dev/null || true

    # Cleanup Ray temporary files
    if [ -n "$RAY_TMPDIR" ] && [ -d "$RAY_TMPDIR" ]; then
        rm -rf "$RAY_TMPDIR"/ray_* 2>/dev/null || true
    fi

    sleep 2
    echo "Ray stopped on $(hostname)"
}

# Function to start Ray head node
start_ray_head() {
    local ray_port=${RAY_PORT:-6379}
    local num_cpus=${RAY_NUM_CPUS:-64}
    local num_gpus=${RAY_NUM_GPUS:-8}

    echo "----------------------------------------"
    echo "Starting Ray head node on $(hostname)"
    echo "  Port: $ray_port"
    echo "  CPUs: $num_cpus"
    echo "  GPUs: $num_gpus"
    echo "----------------------------------------"

    # Stop any existing Ray instance
    stop_ray

    # Start Ray head node
    ray start \
        --head \
        --port=$ray_port \
        --num-cpus=$num_cpus \
        --num-gpus=$num_gpus \
        --include-dashboard=false \
        --disable-usage-stats

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to start Ray head node"
        return 1
    fi

    # Wait for Ray to be ready
    echo "Waiting for Ray head to be ready..."
    local max_wait=30
    local count=0
    until ray status &>/dev/null; do
        sleep 2
        count=$((count + 2))
        if [ $count -ge $max_wait ]; then
            echo "ERROR: Ray head did not start within ${max_wait}s"
            return 1
        fi
        echo "  Waiting... (${count}s)"
    done

    echo "Ray head node is ready."
    ray status
    echo "----------------------------------------"

    return 0
}

# Function to start Ray worker node
start_ray_worker() {
    local ray_head_address="${RAY_HEAD_ADDRESS:-}"
    local num_cpus=${RAY_NUM_CPUS:-64}
    local num_gpus=${RAY_NUM_GPUS:-8}

    if [ -z "$ray_head_address" ]; then
        echo "ERROR: RAY_HEAD_ADDRESS not set for worker node"
        return 1
    fi

    echo "----------------------------------------"
    echo "Starting Ray worker node on $(hostname)"
    echo "  Head address: $ray_head_address"
    echo "  CPUs: $num_cpus"
    echo "  GPUs: $num_gpus"
    echo "----------------------------------------"

    # Stop any existing Ray instance
    stop_ray

    # Start Ray worker node
    ray start \
        --address=$ray_head_address \
        --num-cpus=$num_cpus \
        --num-gpus=$num_gpus \
        --disable-usage-stats

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to start Ray worker node"
        return 1
    fi

    # Wait for Ray to connect
    echo "Waiting for Ray worker to connect..."
    local max_wait=30
    local count=0
    until ray status &>/dev/null; do
        sleep 2
        count=$((count + 2))
        if [ $count -ge $max_wait ]; then
            echo "ERROR: Ray worker did not connect within ${max_wait}s"
            return 1
        fi
        echo "  Waiting... (${count}s)"
    done

    echo "Ray worker node connected."
    echo "----------------------------------------"

    return 0
}

# Function to setup Ray cluster (multi-node)
setup_ray_cluster() {
    echo "=========================================="
    echo "Setting up Ray cluster for multi-node execution"
    echo "=========================================="

    # Get node information from PBS
    if [ -z "$PBS_NODEFILE" ]; then
        echo "ERROR: PBS_NODEFILE not set. Ray cluster setup requires PBS environment."
        return 1
    fi

    # Read nodes from PBS nodefile
    mapfile -t all_nodes < "$PBS_NODEFILE"

    if [ ${#all_nodes[@]} -eq 0 ]; then
        echo "ERROR: No nodes found in PBS_NODEFILE"
        return 1
    fi

    # Get unique nodes (PBS nodefile may have duplicates for multi-core nodes)
    unique_nodes=($(printf "%s\n" "${all_nodes[@]}" | sort -u))

    echo "Detected ${#unique_nodes[@]} unique nodes:"
    printf "  %s\n" "${unique_nodes[@]}"

    # First node is head, rest are workers
    local head_node="${unique_nodes[0]}"
    local worker_nodes=("${unique_nodes[@]:1}")

    echo "Head node: $head_node"
    echo "Worker nodes: ${worker_nodes[*]}"

    # Set Ray configuration
    export RAY_PORT=6379
    export RAY_NUM_CPUS=64
    export RAY_NUM_GPUS=8

    # Start Ray head node
    echo "Starting Ray head on $head_node..."
    if [ "$(hostname)" = "$head_node" ]; then
        # We're on the head node, start directly
        start_ray_head
    else
        # Start remotely via mpiexec
        mpiexec -n 1 -host "$head_node" bash -l -c "
            source '$COMMON_SETUP_SCRIPT'
            setup_environment
            start_ray_head
        "
    fi

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to start Ray head node"
        return 1
    fi

    # Get Ray head address
    export RAY_HEAD_ADDRESS="${head_node}:${RAY_PORT}"
    echo "Ray head address: $RAY_HEAD_ADDRESS"

    # Start Ray workers if we have multiple nodes
    if [ ${#worker_nodes[@]} -gt 0 ]; then
        echo "Starting Ray workers on ${#worker_nodes[@]} nodes..."

        for worker in "${worker_nodes[@]}"; do
            echo "  Starting Ray worker on $worker..."
            if [ "$(hostname)" = "$worker" ]; then
                # We're on this worker node, start directly
                start_ray_worker
            else
                # Start remotely via mpiexec
                mpiexec -n 1 -host "$worker" bash -l -c "
                    source '$COMMON_SETUP_SCRIPT'
                    setup_environment
                    export RAY_HEAD_ADDRESS='$RAY_HEAD_ADDRESS'
                    start_ray_worker
                "
            fi

            if [ $? -ne 0 ]; then
                echo "WARNING: Failed to start Ray worker on $worker"
            fi
        done
    else
        echo "Single-node Ray cluster (head only)"
    fi

    # Allow Ray cluster to stabilize
    sleep 5

    # Verify cluster status
    echo "=========================================="
    echo "Ray Cluster Status:"
    ray status
    echo "=========================================="

    # Check if we have the expected number of nodes
    local expected_nodes=${#unique_nodes[@]}
    local actual_nodes=$(ray status 2>/dev/null | grep -c "node_" || echo "0")

    if [ "$actual_nodes" -ge "$expected_nodes" ]; then
        echo "Ray cluster setup successful: $actual_nodes nodes active"
        return 0
    else
        echo "WARNING: Ray cluster may not have all nodes active"
        echo "  Expected: $expected_nodes"
        echo "  Actual: $actual_nodes"
        echo "Continuing anyway..."
        return 0
    fi
}

################################
# Model Startup Function       #
################################
start_model() {
    local model_name="$1"
    local command="$2"
    local log_file="$3"
    local -n attempt_counter_ref="$4"  # Pass by reference
    local max_attempts="${5:-2}"
    local timeout="${6:-3600}"

    echo "=========================================="
    echo "Starting model: $model_name"
    echo "Log file: $log_file"
    echo "Max attempts: $max_attempts"
    echo "Timeout: ${timeout}s"
    echo "=========================================="

    # Increment attempt counter
    attempt_counter_ref=$((attempt_counter_ref + 1))

    if [ $attempt_counter_ref -gt $max_attempts ]; then
        echo "ERROR: Max attempts ($max_attempts) exceeded"
        return 1
    fi

    echo "Attempt $attempt_counter_ref of $max_attempts"

    # Ensure log directory exists
    local log_dir="$(dirname "$log_file")"
    mkdir -p "$log_dir"

    # Clear previous log
    > "$log_file"

    # Start the model in background
    echo "Executing: $command"
    nohup bash -c "$command" > "$log_file" 2>&1 &
    local pid=$!

    echo "Process started with PID: $pid"
    echo "Monitoring startup (timeout: ${timeout}s)..."

    local start_time=$(date +%s)
    local last_log_time=$start_time

    while true; do
        # Check if process is still running
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: Process $pid exited unexpectedly"
            echo "Last 30 lines of log:"
            tail -n 30 "$log_file"
            return 1
        fi

        # Check for successful startup message
        if [ -f "$log_file" ] && grep -q "INFO:     Application startup complete." "$log_file"; then
            echo "SUCCESS: Model started successfully (PID: $pid)"
            echo "=========================================="
            return 0
        fi

        # Check for common error patterns
        if [ -f "$log_file" ]; then
            if grep -qi "error\|exception\|failed" "$log_file" 2>/dev/null; then
                local current_time=$(date +%s)
                # Only log errors periodically to avoid spam
                if [ $((current_time - last_log_time)) -ge 10 ]; then
                    echo "Detected errors in log (process still running):"
                    grep -i "error\|exception\|failed" "$log_file" | tail -5
                    last_log_time=$current_time
                fi
            fi
        fi

        # Check timeout
        local current_time=$(date +%s)
        local elapsed_time=$((current_time - start_time))

        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "ERROR: Timeout reached after ${timeout}s"
            echo "Killing process $pid..."
            kill -9 "$pid" 2>/dev/null || true
            echo "Last 50 lines of log:"
            tail -n 50 "$log_file"
            return 1
        fi

        # Progress indicator
        if [ $((elapsed_time % 30)) -eq 0 ] && [ $elapsed_time -gt 0 ]; then
            echo "  Still waiting... (${elapsed_time}s elapsed)"
        fi

        sleep 5
    done
}

################################
# Utility Functions            #
################################

# Function to check if Ray is running
is_ray_running() {
    ray status &>/dev/null
    return $?
}

# Function to get Ray cluster info
get_ray_info() {
    if is_ray_running; then
        echo "Ray Cluster Information:"
        ray status
    else
        echo "Ray is not running"
        return 1
    fi
}

# Function to display environment info
show_environment_info() {
    echo "=========================================="
    echo "Environment Information"
    echo "=========================================="
    echo "Hostname: $(hostname)"
    echo "Python: $(which python)"
    echo "Python version: $(python --version 2>&1)"
    echo "Conda env: ${CONDA_DEFAULT_ENV:-not set}"
    echo "vLLM version: ${VLLM_VERSION:-not set}"
    echo "HF_HOME: ${HF_HOME:-not set}"
    echo "Ray status: $(is_ray_running && echo "running" || echo "not running")"
    echo "=========================================="
}

################################
# Main Script Logic            #
################################

# If script is executed directly (not sourced), show help
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    echo "=========================================="
    echo "Sophia Environment Setup Script"
    echo "=========================================="
    echo ""
    echo "This script should be sourced, not executed directly."
    echo ""
    echo "Usage:"
    echo "  source $0"
    echo "  setup_environment"
    echo ""
    echo "Available functions:"
    echo "  - setup_environment        : Setup conda environment and exports"
    echo "  - setup_ray_cluster        : Setup multi-node Ray cluster"
    echo "  - start_model              : Start vLLM model with retry logic"
    echo "  - cleanup_python_processes : Kill all Python/vLLM processes"
    echo "  - stop_ray                 : Stop Ray on current node"
    echo "  - show_environment_info    : Display environment details"
    echo ""
    echo "Environment variables:"
    echo "  - VLLM_VERSION            : vLLM version to use (default: v0.11.0)"
    echo "  - RAY_NUM_CPUS            : CPUs per Ray node (default: 64)"
    echo "  - RAY_NUM_GPUS            : GPUs per Ray node (default: 8)"
    echo "  - RAY_PORT                : Ray head port (default: 6379)"
    echo ""
    exit 0
fi

echo "Sophia environment setup script loaded successfully"
echo "Run 'setup_environment' to initialize the environment"

