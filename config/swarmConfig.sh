# ./scripts/setupSwarm.sh 10.10.0.0/16 10.10.0.1 demmon_network demmon_volume

export SWARM_GATEWAY="10.10.1.1"
export SWARM_SUBNET="10.10.0.0/16"
export SWARM_NET="demmon_network"
export SWARM_VOL="demmon_volume"
export SWARM_VOL_DIR="/tmp/demmon_logs/"

export DOCKER_IMAGE="nmmorais/demmon:latest"

export CONFIG_FILE="config/config50.txt"
export LATENCY_MAP="config/inet100Latencies_x0.04.txt"
export IPS_MAP="config/ips100.txt"