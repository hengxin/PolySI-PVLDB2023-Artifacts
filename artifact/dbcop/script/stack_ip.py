import argparse
import subprocess
import json

parser = argparse.ArgumentParser(description='Get ips of a stack')
parser.add_argument('stack_name', metavar='name',
                    help='name of the docker stack')
parser.add_argument('-q', '--quiet', action="store_true",
                    help='Quiet')


args = parser.parse_args()

stack = args.stack_name


containers = subprocess.run(
    'docker ps -q'.split(), stdout=subprocess.PIPE)

containers = containers.stdout.decode().strip().split("\n")

stack_containers = []

for container in containers:
    info = subprocess.run(
        'docker inspect {}'.format(container).split(),
        stdout=subprocess.PIPE)
    info = json.loads(info.stdout.decode().strip())
    if info[0]['Config']['Labels']['com.docker.stack.namespace'] == stack:
        ip = list(info[0]['NetworkSettings']
                  ['Networks'].values())[0]['IPAddress']
        stack_containers.append((info[0]['Id'], ip))

network_info = subprocess.run(
    'docker network inspect docker_gwbridge'.split(), stdout=subprocess.PIPE)

network_info = json.loads(network_info.stdout.decode().strip())

network_containers = network_info[0]["Containers"]

container_ip = {container: [ip, network_containers[container]["IPv4Address"].rsplit(
    '/', 1)[0]] for container, ip in stack_containers}


if args.quiet:
    print(*(ip[1] for _, ip in container_ip.items()))
else:
    print(json.dumps(container_ip, indent=2))
