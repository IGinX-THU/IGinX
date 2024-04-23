# build image
```bash
build.bat/.sh
```
# run container
```bash
run_docker.bat/.sh -n <container_name(default: iginx-client)> --datadir <datadir_path(default: %SCRIPT_PATH%/data)>
```
<datadir_path> will be mounted as `/iginx_client/data` in container

# run client and get cmd
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