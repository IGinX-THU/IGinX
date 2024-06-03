set VERSION=0.6.0-SNAPSHOT

docker build --build-arg VERSION=%VERSION% --file Dockerfile-no-maven-windows -t iginx-client:%VERSION% ../../client