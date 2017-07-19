#!/bin/sh
#清洗数据

echo "<<<<<<<<<<<<<<<<<清洗...>>>>>>>>>>>>>>>"

hdfs dfs -rm -r /studentDir/xujianrong/output

hadoop jar CleanWebLog-0.0.1-SNAPSHOT-jar-with-dependencies.jar  com.oracle.CleanWebLog.CleanWebLogApp /studentDir/xujianrong/project_data /studentDir/xujianrong/output


echo "<<<<<<<<<<<<<<<<<创建hive表weblog_xu...>>>>>>>>>>>>>>>"
#创建hive表(数据源表)

hive -e "drop table xujianrong.weblog_xu"

hive -e "create TABLE IF NOT EXISTS xujianrong.weblog_xu(timestamp string,user_ip string,user_tracecode string,user_sessionid string,domain string,screen_width string,screen_height string,color_dept string,language string,url string,referrer string,user_agent string,account string,event string,event_data string,day string,hour string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'"


echo "<<<<<<<<<<<<<<<<<导入hive表web_log...>>>>>>>>>>>>>>>"
#将数据导入hive表

hive -e "LOAD DATA  INPATH '/studentDir/xujianrong/output/part-*'  INTO TABLE xujianrong.weblog_xu"


echo "<<<<<<<<<<<<<<<<<创建hive表weblog2_xu...>>>>>>>>>>>>>>>"
#创建查询结果表

hive -e "drop table xujianrong.weblog2_xu"

hive -e "create TABLE IF NOT EXISTS xujianrong.weblog2_xu(Day String,Hour String,PV int,UV int,IP int,visittimes int,Avgpv Double,Avgvisittimes Double) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"


echo "<<<<<<<<<<<<<<<<<导入查询表weblog2_xu...>>>>>>>>>>>>>>>"
#插入查询结果
hive -e "insert INTO xujianrong.weblog2_xu select day,hour,count(*) as pv,count(distinct user_tracecode) as UV,count(distinct user_ip) as IP,count(distinct user_sessionid) as visittimes,cast(count(*)/count(distinct user_tracecode) as decimal(10,2)) as uAvgpv,cast(count(*)/count(distinct user_sessionid) as decimal(10,2)) as Avgvisittimes from xujianrong.weblog_xu group by day,hour"


echo "<<<<<<<<<<<<<<<<<导入mysql...>>>>>>>>>>>>>>>"

rm -rf /var/lib/hadoop-hdfs/xujianrong/weblog2_xu

hdfs dfs -rm -r /user/hdfs/weblog2_xu

mysql -uroot -phadoop <<EOF

use xjrproject

truncate table weblog2_xu;

commit;

exit

EOF


#hive到mysql脚本执行
tableName=$1
hive -e "select * from xujianrong.$tableName" >> $tableName
hdfs dfs -put $tableName /user/hdfs/
sqoop export --connect jdbc:mysql://node1:3306/xjrproject --username temp --password temp --export-dir /user/hdfs/$tableName --table $tableName --input-fields-terminated-by $2

