sudo netstat -tunlp| grep 12201 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary worker1的端口
sudo netstat -tunlp| grep 12202 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary worker2的端口

sudo netstat -tunlp| grep 12401 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby1 worker1的端口
sudo netstat -tunlp| grep 12402 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby2 worker1的端口

sudo netstat -tunlp| grep 12403 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9  #standby1  worker2的端口
sudo netstat -tunlp| grep 12404 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby2 worker2的端口
sudo netstat -tunlp| grep 12200 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary的端口
sudo netstat -tunlp| grep 12301 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker3-primary的端口
#sudo netstat -tunlp| grep 12302 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #worker4-primary的端口

sudo netstat -tunlp| grep 12501 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby1 worker3的端口
sudo netstat -tunlp| grep 12502 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby2 worker3的端口

#sudo netstat -tunlp| grep 12503 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby1 worker4的端口
#sudo netstat -tunlp| grep 12504 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #standby2 worker4的端口

sleep 1m

mkdir worker1-primary
mkdir worker2-primary
mkdir worker1-standby1
mkdir worker1-standby2
mkdir worker2-standby1
mkdir worker2-standby2

cd worker1-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12201  212.64.64.185 12201  & #primary data node
cd ..

cd worker2-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12202   212.64.64.185 12202  & #primary data node
cd ..

cd worker1-standby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12201  212.64.64.185 12401  & #standyby1 data node
cd ..

cd worker1-standby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12201  212.64.64.185 12402  & #standyby2 data node
cd ..

cd worker2-standby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12202   212.64.64.185 12403  & #standyby1 data node
cd ..

cd worker2-standby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12202   212.64.64.185 12404  & #standyby2 data node


