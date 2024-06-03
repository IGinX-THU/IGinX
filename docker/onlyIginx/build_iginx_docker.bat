set VERSION=0.6.0-SNAPSHOT

docker build --file Dockerfile-iginx -t iginx:%VERSION% ../..