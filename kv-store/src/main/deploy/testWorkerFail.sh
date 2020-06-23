sleep 1m

sudo netstat -tunlp| grep 12201 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary worker1的端口

sleep 20s

#recover primary worker1
cd worker1-primary
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12201  212.64.64.185:12201 isrecover & #primary data node