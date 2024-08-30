#! /usr/bin/bash

### Command line argument
ARGS=4                   # Script requires 1 argument.
E_BADARGS=85             # Wrong number of arguments passed to script.
if [ $# -ne "$ARGS" -a $# -ne `expr $ARGS + 1` ]
then
  echo "Usage:   `basename $0` PATH_TO_JAR CONFIG_FILE STAGE SIZE_BATCH [LOG_FILE]"
  echo "Example: `basename $0` target/fuzzyjoin-spark-0.0.2.jar dblp/csx.batch.xml fuzzyjoin/tokensbasic/ridpairsppjoin/recordpairsbasic 1..10 [true/false]"
  exit $E_BADARGS
fi

### Set variables
PATH_TO_JAR=$1           # path to jar file of the project
CONFIG_FILE=$2           # name of the config file for the project
STAGE=$3                 # name of the stage to run (e.g fuzzyjoin, tokensbasic, ridpairsppjoin, recordpairsbasic)
SIZE_BATCH=$4            # number of times to run the stage
D_OPTIONS="spark.driver.extraJavaOptions=-Dfuzzyjoin.savetime=true"
if [ $# -eq `expr $ARGS + 1` -a $5 -eq "false" ]; then
  D_OPTIONS="$D_OPTIONS -Dfuzzyjoin.output.file=false"  # whether to log the output to a file or to the console
fi

### Run the stage(s)
for i in `seq 1 $SIZE_BATCH`
do
spark-submit --class edu.uci.ics.fuzzyjoin.spark.Main --master yarn --conf $D_OPTIONS $PATH_TO_JAR $CONFIG_FILE $STAGE
done