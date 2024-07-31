At least for the SelfJoin (PPJoin+/PK and NestedLoop/BK) you can avoid
duplicate comparisons of the same record pair in different calls of
the reduce function. The idea is to compare records r1 and r2 only if
the "smallest" common prefix token of r1 and r2 equals the first
component of the reduce input key. For the PPJoin+ implementation it
might be checked after applying the length filter.

For DBLP10 and t=0,8 this saves about 9*10^8 comparisons. Furthermore
only 74*10^6 instead of 240*10^6 record pairs are produced what in
turn significantly speeds up the following MapReduce job to join the
matching records.

The patch includes the mentioned optimization
(http://asterix.ics.uci.edu/fuzzyjoin/) for the Self-Join with PPJoin+
Kernel (RIDPairs only). I assume it works only when using individual
tokens. I tested it only with

./hadoop jar ../lib/fuzzyjoin-hadoop-0.0.2-SNAPSHOT.jar ridpairsppjoin
-Dfuzzyjoin.data.dir=/dblp_10 -Dfuzzyjoin.record.data=2,3
-Dfuzzyjoin.similarity.threshold=0.8

what from my understanding uses individual tokens. If applicable, you
might adapt the idea for the cases with Basic-Kernel, RIDRecordPairs
and R-S-Join.

The patch can be applied with :

mkdir tmp&& cp -r fuzzyjoin-0.0.2 tmp&& mv patchfile.patch tmp&& cd
tmp&& patch -p0 -i patchfile.patch

Acknowledgments: Thanks to Lars Kolb for the idea, patch and
instructions.
