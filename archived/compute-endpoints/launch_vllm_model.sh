#!/bin/bash
################################################################################
# vLLM Model Launcher Script
# Version: 2.0
# Description: Launch vLLM models with flexible arguments for single/multi-node
################################################################################

# Usage information
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Required Arguments:
    --model-name MODEL          HuggingFace model name (e.g., meta-llama/Meta-Llama-3.1-8B-Instruct)

Optional Arguments:
    --vllm-version VERSION      vLLM version (default: v0.11.0, e.g., v0.8.2, v0.8.5.post1)
    --tensor-parallel SIZE      Tensor parallel size (default: 8)
    --pipeline-parallel SIZE    Pipeline parallel size (default: 1, requires Ray if >1)
    --multi                     Force multi-node mode with Ray (flag)
    --cuda-visible-devices IDS  Set CUDA_VISIBLE_DEVICES (e.g., "0,1,2,3")
    --port PORT                 Port to listen on (default: 8000)
    --host HOST                 Host to bind to (default: 127.0.0.1)
    --max-model-len LENGTH      Maximum model context length (default: auto)
    --max-num-seqs SEQS         Maximum number of sequences (default: auto)
    --gpu-memory-util RATIO     GPU memory utilization (default: 0.95)
    --chat-template PATH        Path to custom chat template file
    --trust-remote-code         Trust remote code (flag)
    --enable-chunked-prefill    Enable chunked prefill (flag)
    --enable-prefix-caching     Enable prefix caching (flag)
    --enable-auto-tool-choice   Enable auto tool choice (flag)
    --tool-call-parser PARSER   Tool call parser (e.g., llama4_json, llama4_pythonic)
    --served-model-name NAME    Override served model name
    --ssl-keyfile PATH          Path to SSL key file (default: ~/certificates/mykey.key)
    --ssl-certfile PATH         Path to SSL cert file (default: ~/certificates/mycert.crt)
    --disable-log-requests      Disable request logging (flag)
    --disable-log-stats         Disable stats logging (flag)
    --extra-args "ARGS"         Additional vLLM arguments as a quoted string
    --max-attempts NUM          Maximum startup attempts (default: 2)
    --timeout SECONDS           Startup timeout per attempt (default: 3600)
    --framework NAME            Framework identifier for logging (default: vllm)
    --cluster NAME              Cluster name for logging (default: sophia)

Examples:
    # Single node, 8 GPUs
    $0 --model-name meta-llama/Meta-Llama-3.1-8B-Instruct --tensor-parallel 8

    # Single node using specific GPUs
    $0 --model-name meta-llama/Meta-Llama-3.1-8B-Instruct \\
       --tensor-parallel 4 --cuda-visible-devices "0,1,2,3"

    # Force multi-node mode with Ray (even on single node)
    $0 --model-name meta-llama/Meta-Llama-3.1-70B-Instruct \\
       --tensor-parallel 8 --multi

    # Multi-node with pipeline parallelism (auto-enables Ray)
    $0 --model-name meta-llama/Meta-Llama-3.1-405B-Instruct \\
       --tensor-parallel 8 --pipeline-parallel 4 \\
       --max-model-len 16384

    # With custom chat template and tool calling
    $0 --model-name meta-llama/Llama-4-Scout-17B-16E-Instruct \\
       --tensor-parallel 8 \\
       --chat-template /eagle/argonne_tpc/model_weights/chat-templates/tool_chat_template_llama4_pythonic.jinja \\
       --enable-auto-tool-choice \\
       --tool-call-parser llama4_pythonic \\
       --trust-remote-code

    # Using older vLLM version
    $0 --model-name meta-llama/Meta-Llama-3.1-8B-Instruct \\
       --vllm-version v0.8.5.post1 \\
       --tensor-parallel 8

EOF
    exit 1
}

################################
# Default Configuration        #
################################
MODEL_NAME=""
VLLM_VERSION="v0.11.0"
TENSOR_PARALLEL=8
PIPELINE_PARALLEL=1
FORCE_MULTI_NODE=false
CUDA_VISIBLE_DEVICES_ARG=""
PORT=8000
HOST="127.0.0.1"
MAX_MODEL_LEN=""
MAX_NUM_SEQS=""
GPU_MEMORY_UTIL=0.95
CHAT_TEMPLATE=""
TRUST_REMOTE_CODE=false
ENABLE_CHUNKED_PREFILL=false
ENABLE_PREFIX_CACHING=false
ENABLE_AUTO_TOOL_CHOICE=false
TOOL_CALL_PARSER=""
SERVED_MODEL_NAME=""
SSL_KEYFILE="${HOME}/certificates/mykey.key"
SSL_CERTFILE="${HOME}/certificates/mycert.crt"
DISABLE_LOG_REQUESTS=false
DISABLE_LOG_STATS=false
EXTRA_ARGS=""
MAX_ATTEMPTS=2
TIMEOUT=3600
FRAMEWORK="vllm"
CLUSTER="sophia"

################################
# Parse Arguments              #
################################
while [[ $# -gt 0 ]]; do
    case $1 in
        --model-name)
            MODEL_NAME="$2"
            shift 2
            ;;
        --vllm-version)
            VLLM_VERSION="$2"
            shift 2
            ;;
        --tensor-parallel)
            TENSOR_PARALLEL="$2"
            shift 2
            ;;
        --pipeline-parallel)
            PIPELINE_PARALLEL="$2"
            shift 2
            ;;
        --multi)
            FORCE_MULTI_NODE=true
            shift
            ;;
        --cuda-visible-devices)
            CUDA_VISIBLE_DEVICES_ARG="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --max-model-len)
            MAX_MODEL_LEN="$2"
            shift 2
            ;;
        --max-num-seqs)
            MAX_NUM_SEQS="$2"
            shift 2
            ;;
        --gpu-memory-util)
            GPU_MEMORY_UTIL="$2"
            shift 2
            ;;
        --chat-template)
            CHAT_TEMPLATE="$2"
            shift 2
            ;;
        --trust-remote-code)
            TRUST_REMOTE_CODE=true
            shift
            ;;
        --enable-chunked-prefill)
            ENABLE_CHUNKED_PREFILL=true
            shift
            ;;
        --enable-prefix-caching)
            ENABLE_PREFIX_CACHING=true
            shift
            ;;
        --enable-auto-tool-choice)
            ENABLE_AUTO_TOOL_CHOICE=true
            shift
            ;;
        --tool-call-parser)
            TOOL_CALL_PARSER="$2"
            shift 2
            ;;
        --served-model-name)
            SERVED_MODEL_NAME="$2"
            shift 2
            ;;
        --ssl-keyfile)
            SSL_KEYFILE="$2"
            shift 2
            ;;
        --ssl-certfile)
            SSL_CERTFILE="$2"
            shift 2
            ;;
        --disable-log-requests)
            DISABLE_LOG_REQUESTS=true
            shift
            ;;
        --disable-log-stats)
            DISABLE_LOG_STATS=true
            shift
            ;;
        --extra-args)
            EXTRA_ARGS="$2"
            shift 2
            ;;
        --max-attempts)
            MAX_ATTEMPTS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --framework)
            FRAMEWORK="$2"
            shift 2
            ;;
        --cluster)
            CLUSTER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$MODEL_NAME" ]; then
    echo "ERROR: --model-name is required"
    usage
fi

################################
# Source Environment Setup     #
################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_SETUP_SCRIPT="${ENV_SETUP_SCRIPT:-${SCRIPT_DIR}/sophia_env_setup_with_ray.sh}"

if [ ! -f "$ENV_SETUP_SCRIPT" ]; then
    # Try alternate location
    ENV_SETUP_SCRIPT="/home/openinference_svc/sophia_env_setup_with_ray.sh"
fi

if [ ! -f "$ENV_SETUP_SCRIPT" ]; then
    echo "ERROR: Environment setup script not found: $ENV_SETUP_SCRIPT"
    exit 1
fi

echo "Sourcing environment setup script: $ENV_SETUP_SCRIPT"
source "$ENV_SETUP_SCRIPT"

# Set vLLM version before environment setup
export VLLM_VERSION="$VLLM_VERSION"

# Setup environment
setup_environment

################################
# Determine Execution Mode     #
################################
MULTI_NODE=false

# Multi-node mode is enabled if:
# 1. User explicitly specifies --multi flag
# 2. Pipeline parallelism is used (PP > 1)
if [ "$FORCE_MULTI_NODE" = true ]; then
    MULTI_NODE=true
    echo "Multi-node mode explicitly enabled (--multi flag)"
elif [ "$PIPELINE_PARALLEL" -gt 1 ]; then
    MULTI_NODE=true
    echo "Pipeline parallelism detected (PP=$PIPELINE_PARALLEL), multi-node mode required"
fi

echo "Execution mode: $([ "$MULTI_NODE" = true ] && echo "MULTI-NODE (Ray)" || echo "SINGLE-NODE (multiprocessing)")"

################################
# Setup Ray if Multi-Node      #
################################
if [ "$MULTI_NODE" = true ]; then
    echo "Setting up Ray cluster for multi-node execution..."

    if ! setup_ray_cluster; then
        echo "ERROR: Failed to setup Ray cluster"
        exit 1
    fi

    # Use Ray backend for multi-node or pipeline parallelism
    DISTRIBUTED_BACKEND="ray"
else
    # Use multiprocessing backend for single-node tensor parallelism
    DISTRIBUTED_BACKEND="mp"
fi

################################
# Build vLLM Command           #
################################
echo "Building vLLM command..."

# Start with base command
VLLM_CMD="vllm serve ${MODEL_NAME}"
VLLM_CMD="${VLLM_CMD} --host ${HOST}"
VLLM_CMD="${VLLM_CMD} --port ${PORT}"
VLLM_CMD="${VLLM_CMD} --tensor-parallel-size ${TENSOR_PARALLEL}"

if [ "$PIPELINE_PARALLEL" -gt 1 ]; then
    VLLM_CMD="${VLLM_CMD} --pipeline-parallel-size ${PIPELINE_PARALLEL}"
fi

if [ -n "$DISTRIBUTED_BACKEND" ]; then
    VLLM_CMD="${VLLM_CMD} --distributed-executor-backend ${DISTRIBUTED_BACKEND}"
fi

if [ -n "$MAX_MODEL_LEN" ]; then
    VLLM_CMD="${VLLM_CMD} --max-model-len ${MAX_MODEL_LEN}"
fi

if [ -n "$MAX_NUM_SEQS" ]; then
    VLLM_CMD="${VLLM_CMD} --max-num-seqs ${MAX_NUM_SEQS}"
fi

VLLM_CMD="${VLLM_CMD} --gpu-memory-utilization ${GPU_MEMORY_UTIL}"

if [ "$TRUST_REMOTE_CODE" = true ]; then
    VLLM_CMD="${VLLM_CMD} --trust-remote-code"
fi

if [ "$ENABLE_CHUNKED_PREFILL" = true ]; then
    VLLM_CMD="${VLLM_CMD} --enable-chunked-prefill"
fi

if [ "$ENABLE_PREFIX_CACHING" = true ]; then
    VLLM_CMD="${VLLM_CMD} --enable-prefix-caching"
fi

if [ "$ENABLE_AUTO_TOOL_CHOICE" = true ]; then
    VLLM_CMD="${VLLM_CMD} --enable-auto-tool-choice"
fi

if [ -n "$TOOL_CALL_PARSER" ]; then
    VLLM_CMD="${VLLM_CMD} --tool-call-parser ${TOOL_CALL_PARSER}"
fi

if [ -n "$CHAT_TEMPLATE" ]; then
    if [ -f "$CHAT_TEMPLATE" ]; then
        VLLM_CMD="${VLLM_CMD} --chat-template ${CHAT_TEMPLATE}"
    else
        echo "WARNING: Chat template file not found: $CHAT_TEMPLATE"
    fi
fi

if [ -n "$SERVED_MODEL_NAME" ]; then
    VLLM_CMD="${VLLM_CMD} --served-model-name ${SERVED_MODEL_NAME}"
fi

if [ "$DISABLE_LOG_REQUESTS" = true ]; then
    VLLM_CMD="${VLLM_CMD} --disable-log-requests"
fi

if [ "$DISABLE_LOG_STATS" = true ]; then
    VLLM_CMD="${VLLM_CMD} --disable-log-stats"
fi

# SSL certificates
if [ -f "$SSL_KEYFILE" ] && [ -f "$SSL_CERTFILE" ]; then
    VLLM_CMD="${VLLM_CMD} --ssl-keyfile ${SSL_KEYFILE}"
    VLLM_CMD="${VLLM_CMD} --ssl-certfile ${SSL_CERTFILE}"
else
    echo "WARNING: SSL certificates not found, running without SSL"
fi

# Add extra arguments
if [ -n "$EXTRA_ARGS" ]; then
    VLLM_CMD="${VLLM_CMD} ${EXTRA_ARGS}"
fi

################################
# Setup Logging                #
################################
# Create clean model name for log file
MODEL_NAME_CLEAN=$(echo "$MODEL_NAME" | sed 's/[^a-zA-Z0-9._-]/_/g')
LOG_FILE="${PWD}/logfile_${CLUSTER}-${FRAMEWORK}-${MODEL_NAME_CLEAN}_$(hostname).log"

echo "=========================================="
echo "vLLM Launch Configuration"
echo "=========================================="
echo "Model: $MODEL_NAME"
echo "vLLM Version: $VLLM_VERSION"
echo "Tensor Parallel: $TENSOR_PARALLEL"
echo "Pipeline Parallel: $PIPELINE_PARALLEL"
echo "Distributed Backend: ${DISTRIBUTED_BACKEND:-multiprocessing}"
echo "Host: $HOST"
echo "Port: $PORT"
echo "Max Model Length: ${MAX_MODEL_LEN:-auto}"
echo "GPU Memory Util: $GPU_MEMORY_UTIL"
if [ -n "$CUDA_VISIBLE_DEVICES_ARG" ]; then
    echo "CUDA Visible Devices: $CUDA_VISIBLE_DEVICES_ARG"
fi
echo "Log File: $LOG_FILE"
echo "=========================================="
echo "Command: $VLLM_CMD"
echo "=========================================="

################################
# Set CUDA_VISIBLE_DEVICES     #
################################
if [ -n "$CUDA_VISIBLE_DEVICES_ARG" ]; then
    export CUDA_VISIBLE_DEVICES="$CUDA_VISIBLE_DEVICES_ARG"
    echo "Set CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
fi

################################
# Start the Model              #
################################
retry_counter=0

echo "Starting model with retry logic (max attempts: $MAX_ATTEMPTS)..."

while true; do
    if start_model "$MODEL_NAME" "$VLLM_CMD" "$LOG_FILE" retry_counter "$MAX_ATTEMPTS" "$TIMEOUT"; then
        echo "=========================================="
        echo "Model started successfully!"
        echo "=========================================="
        echo "Model: $MODEL_NAME"
        echo "Endpoint: https://$(hostname):${PORT}"
        echo "Log: $LOG_FILE"
        echo "=========================================="

        # Keep the script running to maintain the model process
        echo "Model is running. Press Ctrl+C to stop."
        wait
        exit 0
    else
        echo "Failed to start model on attempt $retry_counter"
        if [ $retry_counter -ge $MAX_ATTEMPTS ]; then
            echo "=========================================="
            echo "ERROR: Failed to start model after $MAX_ATTEMPTS attempts"
            echo "=========================================="
            echo "Check log file: $LOG_FILE"
            exit 1
        fi
        echo "Retrying..."
    fi
done

