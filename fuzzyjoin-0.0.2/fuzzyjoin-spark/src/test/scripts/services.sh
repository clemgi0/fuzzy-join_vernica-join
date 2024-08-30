#! /usr/bin/bash

### Command line argument
ARGS=1                   # Script requires 1 argument.
E_BADARGS=85             # Wrong number of arguments passed to script.
if [ $# -ne "$ARGS" ]
then
  echo "Usage:   `basename $0` status"
  echo "Example: `basename $0` start/stop"
  exit $E_BADARGS
fi
STATUS=$1                       # start or stop

DFS_SCRIPT="${STATUS}-dfs.sh"   # start-dfs.sh or stop-dfs.sh
YARN_SCRIPT="${STATUS}-yarn.sh" # start-yarn.sh or stop-yarn.sh

### Start or stop services
if [ $STATUS == "start" ]
then
    echo "Starting services..."
    sudo service ssh $STATUS
    $DFS_SCRIPT
    $YARN_SCRIPT
else
    echo "Stopping services..."
    $YARN_SCRIPT
    $DFS_SCRIPT
    sudo service ssh $STATUS
fi
