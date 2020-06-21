sudo netstat -tunlp| grep 12200 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary的端口
sudo netstat -tunlp| grep 12301 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker3-primary的端口
sudo netstat -tunlp| grep 12302 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker4-primary的端口

sudo netstat -tunlp| grep 12501 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standyby1 worker3的端口
sudo netstat -tunlp| grep 12502 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standyby2 worker3的端口

sudo netstat -tunlp| grep 12503 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standyby1 worker4的端口
sudo netstat -tunlp| grep 12504 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standyby2 worker4的端口

mkdir ScaleOutTest
cd ScaleOutTest
sudo rm -rf log4j2.log

sleep 30s
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar doPUTandGET & #起了PRIMARY并进行PUT GET操作

cd ..
sleep 5m

mkdir worker3-primary
mkdir worker4-primary
mkdir worker3-standyby1
mkdir worker3-standyby2
mkdir worker4-standyby1
mkdir worker4-standyby2

cd worker3-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12301 \
212.64.64.185 12301 &  #primary data node
cd ..

cd worker4-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
212.64.64.185 12302 & #primary data node
cd ..

cd worker3-standyby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12301  212.64.64.185 12501  & #standyby1 data node
cd ..

cd worker3-standyby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12301  212.64.64.185 12502  & #standyby2 data node
cd ..

cd worker4-standyby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
212.64.64.185 12503 & #primary data node
cd ..

cd worker4-standyby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
212.64.64.185 12504 & #primary data node