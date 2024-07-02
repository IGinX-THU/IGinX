#!/bin/bash
# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

if [ -z "${IGINX_CLI_HOME}" ]; then
  export IGINX_CLI_HOME="$(
    cd "$(dirname "$0")"/..
    pwd
  )"
fi

MAIN_CLASS=cn.edu.tsinghua.iginx.client.IginxClient

CLASSPATH=""
for f in ${IGINX_CLI_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

if [ -n "$JAVA_HOME" ]; then
  for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
    if [ -x "$java" ]; then
      JAVA="$java"
      break
    fi
  done
else
  JAVA=java
fi

PARAMETERS=("$@")

# Added parameters when default parameters are missing

# sh version
case "${PARAMETERS[@]}" in
*"-fs "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-fs" "1000" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-pw "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-pw" "root" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-u "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-u" "root" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-p "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-p" "6888" "${PARAMETERS[@]}") ;;
esac
case "${PARAMETERS[@]}" in
*"-h "*) PARAMETERS=$PARAMETERS ;;
*) PARAMETERS=("-h" "127.0.0.1" "${PARAMETERS[@]}") ;;
esac

exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "${PARAMETERS[@]}"

exit $?