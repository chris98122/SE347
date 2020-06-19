mkdir ScaleOutTest
cd ScaleOutTest
sudo rm -rf log4j2.log
sudo netstat -tunlp| grep 12200 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary的端口
sudo netstat -tunlp| grep 12301 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker3的端口
sudo netstat -tunlp| grep 12302 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker4的端口
sleep 1m
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar ScaleOutTest & #起了PRIMARY

cd ..
sleep 1m

mkdir worker3
cd worker3
sudo rm -rf log4j2.log
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12301 &

cd ..
mkdir worker4
cd worker4
sudo rm -rf log4j2.log
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 &
