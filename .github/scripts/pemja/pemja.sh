#!/bin/sh

set -e

sh -c "git clone https://github.com/shinyano/pemja.git"

sh -c "cd pemja"

sh -c "mvn clean install -DskipTests"

sh -c "pip install -r dev/dev-requirements.txt"

sh -c "python setup.py sdist"

sh -c "pip install dist/*.tar.gz"