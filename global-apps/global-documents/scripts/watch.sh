#!/bin/bash

#
# Uses inotify to to watch ./front/src folder and run npm build on changes
#

# Go to main project folder from anywhere
cd $(dirname "$(readlink $(test $(uname -s) = 'Linux' && echo "-f") "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")/..

cd front

while inotifywait -qr src/ -e modify; do
    npm run-script build
done
