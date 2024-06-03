set VERSION=0.6.0-SNAPSHOT

docker build --build-arg VERSION=%VERSION% --file Dockerfile -t iginx-client:%VERSION% ../../client