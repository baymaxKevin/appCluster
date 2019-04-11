# appCluster
移动手机安装应用列表反映不同用户群体喜爱，可以抽象提取为用户画像特征<br>
训练样本以7天30多G，dwu=2600多万，启动脚本如下：<br>
```Bash
spark2-submit  --master yarn --deploy-mode cluster --class com.meiyou.cluster.AppCluster --queue lee --executor-memory 8G --executor-cores 4 --driver-memory 8G --num-executors 50 --driver-cores 1 --conf spark.default.parallelism=600 hdfs://lee:8020/jars/app_cluster.jar 50 20 128 129
```
离线测试样本为每天数据，启动脚本如下：<br>
```Bash
spark2-submit  --master yarn --deploy-mode cluster --class com.meiyou.cluster.OnLineCluster --queue lee --executor-memory 4G --executor-cores 8 --driver-memory 16G --num-executors 20 --driver-cores 10 --conf spark.default.parallelism=600 --conf spark.kryoserializer.buffer.max=128m hdfs://bjn-1:8020/jars/app_cluster.jar 20 $(date +"%Y%m%d" -d "-1 days")
```
实时测试样本batch=30，启动脚本如下：<br>
```Bash
spark2-submit  --master yarn --deploy-mode cluster --class com.meiyou.cluster.AppClusterDstream --queue lee --executor-memory 4G --executor-cores 4 --driver-memory 8G --num-executors 5 --driver-cores 4 --conf spark.kryoserializer.buffer.max=128m hdfs://bjn-1:8020/jars/app_cluster.jar 30
```
