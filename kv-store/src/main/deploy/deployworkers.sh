mkdir worker1
mkdir worker2
cd worker1
sudo netstat -tunlp| grep 12200 |grep -v grep| awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9
sudo netstat -tunlp| grep 12201 |grep -v grep| awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181 212.64.64.185 12200 &
cd ..
cd worker2
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181 212.64.64.185 12201 &