# How to use IGinX client script in bash / CMD
This is a manual to refer when you want to call IGinX client script all by bash on Linux or CMD/PowerShell on Windows.

IGinX client is a program in script that automatically connect to configure-allowed IGinX service, allows users to write and execute statements in IGinX.
## Location of IGinX client scripts
After compilation([read this doc to compile](./quickStarts/IGinXBySource.md)), IGinX client script(.sh & .bat) will be generated and stored in `{IGINX_ROOT}/client/target/iginx-client-{version}/sbin `.
## Call script and pass arguments
**Ensure that on Linux systems, your shell script has execution permissions (use the chmod +x start_cli.sh command to grant execution rights).**

The script accepts specific command-line arguments for configuration purposes. Below is the list of arguments accepted by the script along with their descriptions:

### Argument list

- `-e,--execute <arg>` Execute statements(optional)
- `-h,--host <arg>` Host address of IGinX service (optional, default 127.0.0.1)
- `-p,--port <arg>` Port of IGinX service (optional, default 6888)
- `-u,--username <arg>` User name (optional, default "root")
- `-pw,--password <arg>` Password (optional, default "root")
- `-fs,--fetch_size <arg>` Fetch size per query (optional, default 1000)
- `-help` Display help information(optional)

### Examples

#### Linux usage

```bash
./start_cli.sh -h 10.11.178.3 -p 6888
# connect to IGinX service that is deployed on 10.11.178.3:6888
```

```bash
./start_cli.sh -e 'SHOW COLUMNS;SHOW CLUSTER INFO;'
# connect to IGinX service on 127.0.0.1:6888 and execute two statement by order:
#     1. SHOW COLUMNS;
#     2. SHOW CLUSTER INFO;
# note that every single statement needs to be ended with a semicolon(;)
```

#### Windows usage

```bash
start_cli.bat -h 10.11.178.3 -p 6888
# connect to IGinX service that is deployed on 10.11.178.3:6888
```

```bash
start_cli.bat -e 'SHOW COLUMNS;SHOW CLUSTER INFO;'
# connect to IGinX service on 127.0.0.1:6888 and execute two statement by order:
#     1. SHOW COLUMNS;
#     2. SHOW CLUSTER INFO;
# note that every single statement needs to be ended with a semicolon(;)
```