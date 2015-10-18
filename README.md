# RecSpark
A recommendation system written by scala, it  runs on spark clusters.

content
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.box.ContentRec --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494
behavior
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.box.BehaviorRec --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494 --timeSpan 10
newcontent
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.box.NewContent --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494
topN
一个月1min30s
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.box.ContentTopN --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494 --timeSpan 10
userproperty
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.box.UserProperty --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494 --timeSpan 10

bgctv
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.tv.BgcTV --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494

ottChanneltopN
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.ott.OttChannelTopN --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494

ottbehavior
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.ott.OttBehaviorRec --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494

otttopN
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.ott.OttContentTopN --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494

ottContentRec
sh /opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/bin/spark-submit --master local[*] --num-executors 10 --executor-memory 14g --driver-memory 10g --class com.ctvit.ott.OttContentRec --jars /var/ire/ext/mysql-connector-java-5.1.7-bin.jar,/var/ire/ext/jedis-2.1.0.jar,/var/ire/ext/json-lib-2.3-jdk15.jar,/var/ire/ext/ezmorph-1.0.6.jar,/var/ire/ext/spark-examples-1.3.0-hadoop2.4.0.jar /var/ire/IRE.jar --taskId 55366f7f-45e5-496c-99c4-9151533b8494









