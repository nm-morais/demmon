# ./scripts/setupSwarm.sh 10.10.0.0/16 10.10.0.1 demmon_network demmon_volume

export SWARM_GATEWAY="10.10.1.1"
export SWARM_SUBNET="10.10.0.0/16"
export SWARM_NET="demmon_network"
export SWARM_VOL="demmon_volume"
export SWARM_VOL_DIR="/tmp/demmon_logs/"

export DOCKER_IMAGE="nmmorais/demmon:latest"

export CONFIG_FILE="config/akos100_config.txt"
export LATENCY_MAP="config/akos100_latencies.txt"
export IPS_MAP="config/akos100_ips.txt"