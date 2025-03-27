port=$1

pwd

docker ps

# # stop the Dameng database
# docker stop dm8-$port

# check if the container is stopped
if docker ps -a | grep -q "dm8-$port"; then
  echo "Container dm8-$port is still running, stopping it"
  docker stop "dm8-$port" || {
    echo "ERROR: Failed to stop container"
    exit 1
  }
  echo "Container stopped successfully"
else
  echo "Container dm8-$port is already stopped"
fi