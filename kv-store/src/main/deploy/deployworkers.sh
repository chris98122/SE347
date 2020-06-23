sh  ./kv-store/target/clear.sh
sleep 15s
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
 212.64.64.185:12201  212.64.64.185:12201 notrecover & #primary data node
cd ..

cd worker2-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12202   212.64.64.185:12202   notrecover & #primary data node
cd ..

cd worker1-standby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12201  212.64.64.185:12401   notrecover & #standyby1 data node
cd ..

cd worker1-standby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12201  212.64.64.185:12402   notrecover & #standyby2 data node
cd ..

cd worker2-standby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12202   212.64.64.185:12403  notrecover & #standyby1 data node
cd ..

cd worker2-standby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185:12202   212.64.64.185:12404  notrecover & #standyby2 data node


