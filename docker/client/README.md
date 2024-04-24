# build image

```bash
build.bat/.sh
```

# run container

Assume a docker network `docker-cluster-iginx` has been created.

```bash
docker network create -d bridge --attachable --subnet 172.40.0.0/16 docker-cluster-iginx
```

Run the container:

```bash
run_docker.bat/.sh -n <container_name(default: iginx-client)> --datadir <datadir_path> --ip <ip> --net <net>
```

- <datadir_path> will be mounted as `/iginx_client/data` in container(default: %SCRIPT_PATH%/data)
- --net: client container's net(default:docker-cluster-iginx). You can change with any customized docker network.
- --ip: client's ip in net(default:172.40.0.3)

# run client and get the terminal

```bash
# default config
docker exec -it iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal

# full config
docker exec -it iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal -p 6888 -u root -pw root
```

- **-h host.docker.internal**: to access IGinX that runs on host.
- **iginx-client** should be replaced by customized container name.
- **-p 6888 -u root -pw root**: optional. Default values are set.

# run client and execute statements

```bash
docker exec iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal -e "<statements>"
```

Don't forget the DOUBLE-QUOTES around statements. Statements are separated and ended with semicolon.
