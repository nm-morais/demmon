# ./scripts/setupSwarm.sh 10.10.0.0/16 10.10.0.1 demmon_network demmon_volume

export SWARM_GATEWAY="10.10.1.1"
export SWARM_SUBNET="10.10.0.0/16"
export SWARM_NET="dummies-network"
export SWARM_VOL="demmon_volume"
export SWARM_VOL_DIR="/tmp/demmon_logs/"

export DOCKER_IMAGE="brunoanjos/demmon:latest"

export CONFIG_FILE="config/banjos_config.txt"
export LATENCY_MAP="config/latency_map.txt"
export IPS_MAP="config/banjos_ips_config.txt"
