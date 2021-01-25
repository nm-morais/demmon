#!/usr/bin/env python3

import argparse
from netaddr import IPNetwork
import json
import os
import subprocess
import signal
import sys


config_file = "config/generated_config.txt"
default_nr_landmarks = 3
n_nodes_generated_conf = 25
network = "demmon_network"
cidr_provided = "10.10.0.0/16"
vol_dir = "/tmp/demmon_logs/"
vol_name = "demmon_volume"
swarm_gateway = "10.10.1.1"
image_name = "nmmorais/demmon:latest"
latency_map = "config/bruno_100_latencies.txt"


def assign_env_vars(env):
    env["SWARM_GATEWAY"] = swarm_gateway
    env["SWARM_SUBNET"] = cidr_provided
    env["SWARM_NET"] = network
    env["SWARM_VOL"] = vol_name
    env["SWARM_VOL_DIR"] = vol_dir
    env["DOCKER_IMAGE"] = image_name
    env["CONFIG_FILE"] = config_file
    env["LATENCY_MAP"] = latency_map


def main():
    args = parseArgs()
    nodeList = [nodeName for nodeName in args.nodes.split(" ")]
    print(f"NodeList: {nodeList}")
    print(f"args: {args}")

    if args.generate:
        writeGeneratedConf(nodeList)

    if args.create_swarm:
        start_swarm(nodeList)

    if args.teardown_swarm:
        teardown(nodeList)

    if args.deploy:
        deploy(nodeList, args.landmarks)

    if args.check:
        check(nodeList)

    if args.stop:
        stop(nodeList)


def parseArgs():
    parser = argparse.ArgumentParser(description='demmon main script')

    parser.add_argument('--create', dest='create_swarm',
                        action='store_true', help='start swam')

    parser.add_argument('--teardown', dest='teardown_swarm',
                        action='store_true', help='teardown swam')

    parser.add_argument('--deploy', dest='deploy',
                        action='store_true', help='deploy configuration')

    parser.add_argument('--check', dest='check',
                        action='store_true', help='check for errors in running containers')

    parser.add_argument('--generate', dest='generate',
                        action='store_true', help='generate configurations')

    parser.add_argument('--stop', dest='stop',
                        action='store_true', help='stop configurations')

    parser.add_argument("--landmarks", type=int,
                        help="the number of landmarks", required=False, default=default_nr_landmarks, dest="landmarks")

    parser.add_argument('--nodes',
                        metavar='nodes',
                        action='store',
                        required=True,
                        type=str,
                        help='the node list (separated by spaces)')

    return parser.parse_args()


def check(nodeList):
    tmp_dir = "/home/nunomorais/demmon_logs/"
    gather_logs(nodeList, vol_dir, tmp_dir)
    for node_folder in os.listdir(tmp_dir):
        print(node_folder)
        node_path = "{}/{}".format(tmp_dir, node_folder)
        for node_file in os.listdir(node_path):
            parse_for_errors(f"{node_path}/{node_file}")


def parse_for_errors(file):
    f = open(file, "r")
    lines = f.readlines()
    for line in lines:
        if "error" in line:
            print(line)
    return


def start_swarm(nodeList):
    node_list_str = join_str_arr(nodeList, " ")
    setup_cmd = f"bash scripts/setupSwarm.sh {node_list_str}"
    d = dict(os.environ)
    assign_env_vars(d)
    run_cmd_with_try(setup_cmd, env=d, stdout=sys.stdout)
    return


def teardown(nodeList):
    node_list_str = join_str_arr(nodeList, " ")
    stop_cmd = f"bash scripts/stopSwarm.sh {node_list_str}"
    d = dict(os.environ)
    assign_env_vars(d)
    run_cmd_with_try(stop_cmd, env=d, stdout=sys.stdout)
    return


def stop(nodeList):
    node_list_str = join_str_arr(nodeList, " ")
    deploy_cmd = f"bash scripts/stopContainers.sh {node_list_str}"
    d = dict(os.environ)
    assign_env_vars(d)
    run_cmd_with_try(deploy_cmd, env=d, stdout=sys.stdout)
    return


def stop(nodeList):
    node_list_str = join_str_arr(nodeList, " ")
    deploy_cmd = f"bash scripts/stopContainers.sh {node_list_str}"
    d = dict(os.environ)
    assign_env_vars(d)
    run_cmd_with_try(deploy_cmd, env=d, stdout=sys.stdout)
    return


def deploy(nodeList, landmarks_nr):
    node_list_str = join_str_arr(nodeList, " ")
    deploy_cmd = f"./scripts/setupContainers.sh {node_list_str}"
    d = dict(os.environ)
    assign_env_vars(d)
    landmarks = []
    f = open(config_file, "r")
    for i in range(landmarks_nr):
        landmarks.append(f.readline().split(" ")[0])

    print("deploying with landmarks: {}".format(join_str_arr(landmarks, " ")))
    d["LANDMARKS"] = join_str_arr(landmarks, ";")
    run_cmd_with_try(deploy_cmd, env=d, stdout=sys.stdout)
    return


def join_str_arr(arr, separator):
    return separator.join(arr)


def writeGeneratedConf(nodeList):
    ips = [str(ip) for ip in IPNetwork(cidr_provided)]
    # Ignore first two IPs since they normally are the NetAddr and the Gateway, and ignore last one since normally it's the
    # broadcast IP
    ips = ips[2:-1]
    entrypoints = setup_anchors(nodeList)
    print(f"entrypoints: {entrypoints}")
    f = open(config_file, "w")
    added = 0
    for i, ip in enumerate(reversed(ips)):
        if ip not in entrypoints:
            added += 1
            f.write(f"{ip} node{i}\n")

        if added == n_nodes_generated_conf:
            break

    print(f"wrote configuration to file: {config_file}")
    f.close()


def gather_logs(nodeList, source_folder, dest_folder):
    for node in nodeList:
        copyLogsFromNodeCmd = f"rsync -raz {node}:{source_folder} {dest_folder}"
        run_cmd_with_try(cmd=copyLogsFromNodeCmd, stdout=sys.stdout)


def exec_cmd_on_node_with_output(cmd, node):
    remote_cmd = f"oarsh {node} -- {cmd}"
    (status, out) = subprocess.getstatusoutput(remote_cmd)
    if status != 0:
        print(out)
        exit(1)
    return out


def run_cmd_with_try(cmd, env=dict(os.environ), stdout=subprocess.DEVNULL):
    print(f"Running | {cmd} | LOCAL")
    cp = subprocess.run(cmd, shell=True, stdout=stdout, env=env)
    if cp.stderr is not None:
        raise Exception(cp.stderr)


def exec_cmd_on_node(node, cmd, env={}):
    path_var = os.environ["PATH"]
    remote_cmd = f"oarsh {node} -- 'PATH=\"{path_var}\" && {cmd}'"
    run_cmd_with_try(remote_cmd, env)


def setup_anchors(nodes):
    entrypoints_ips = set()
    for node in nodes:
        print(f"Setting up anchor at {node}")
        anchor_cmd = f"docker run -d --name=anchor-{node} --network {network} alpine sleep 30m"
        exec_cmd_on_node(node, anchor_cmd)

        """
        Output is like:
        "lb-swarm-network": {
            "Name": "swarm-network-endpoint",
            "EndpointID": "ab543cead9c04275a95df7632165198601de77c183945f2a6ab82ed77a68fdd3",
            "MacAddress": "02:42:c0:a8:a0:03",
            "IPv4Address": "192.168.160.3/20",
            "IPv6Address": ""
        }
        so we split at max once thus giving us only the value and not the key
        """

        get_entrypoint_cmd = f"docker network inspect {network} | grep 'lb-{network}' -A 6"
        output = exec_cmd_on_node_with_output(
            get_entrypoint_cmd, node).strip().split(" ", 1)[1]

        entrypoint_json = json.loads(output)

        entrypoints_ips.add(entrypoint_json["IPv4Address"].split("/")[0])
        get_anchor_cmd = f"docker network inspect {network} | grep 'anchor' -A 5 -B 1"
        output = exec_cmd_on_node_with_output(
            get_anchor_cmd, node).strip().split(" ", 1)[1]
        if output[-1] == ",":
            output = output[:-1]

        anchor_json = json.loads(output)
        entrypoints_ips.add(anchor_json["IPv4Address"].split("/")[0])

    return entrypoints_ips


if __name__ == "__main__":
    main()
