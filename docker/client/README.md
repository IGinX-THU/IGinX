# build image

```bash
build.sh(unix) 
# or
build-no-maven.bat(windows)
# or
build.bat(linux container on WindowsOS)
```

On Windows, user need to run `mvn clean package -Dmaven.test.skip=true -P-format` in root directory before
running the script.

If user can use linux container on their windowsOS, they can run `build.bat` without `mvn package`

# run container

Run the container:

```bash
run_docker.bat/.sh -n <container_name(default: iginx-client)> --datadir <datadir_path>
```

- <datadir_path> will be mounted as `/iginx_client/data` in container(default: %SCRIPT_PATH%/data)

# run client and get the terminal

```bash
# minimum config(if IGinX server runs on the host machine)
docker exec -it iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal

# full config
docker exec -it iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal -p 6888 -u root -pw root
```

- **-h host.docker.internal**: to access IGinX that runs on host. Replace it with real ip address if necessary.
- **iginx-client** should be replaced by customized container name.
- **-p 6888 -u root -pw root**: optional. Default values are set.

# run client and execute statements

```bash
# example
docker exec iginx-client /iginx_client/sbin/start_cli.sh -h host.docker.internal -e "show cluster info; show functions;"
```

Don't forget the DOUBLE-QUOTES around statements. Statements are separated and ended with semicolon.
