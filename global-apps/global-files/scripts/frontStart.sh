#Start java server and nodemon to watch ./front/src folder

#!/bin/bash
if [ "$(uname -s)" = 'Linux' ]; then
  BASE_DIR=$(dirname "$(readlink -f "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")
else
  BASE_DIR=$(dirname "$(readlink "$0" || echo "$(echo "$0" | sed -e 's,\\,/,g')")")
fi
cd $BASE_DIR/..

cd front && nodemon --watch src --exec npm run-script build &  FRONT=$!
mvn compile exec:java &  SERVER=$!
wait $FRONT
wait $SERVER
