mkdir ScaleOutTest
cd ScaleOutTest
sudo rm -rf log4j2.log

sleep 30s
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar doPUTandGET & #起了PRIMARY并进行PUT GET操作

cd ..
sleep 20s

mkdir worker3-primary
mkdir worker4-primary
mkdir worker3-standby1
mkdir worker3-standby2
mkdir worker4-standby1
mkdir worker4-standby2

cd worker3-primary
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12301 \
212.64.64.185 12301 &  #primary data node
cd ..

#cd worker4-primary
#sudo rm -rf log4j2.log
#sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
#java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
#112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
#212.64.64.185 12302 & #primary data node
#cd ..

cd worker3-standby1
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12301  212.64.64.185 12501  & #standyby1 data node
cd ..

cd worker3-standby2
sudo rm -rf log4j2.log
sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 \
 212.64.64.185 12301  212.64.64.185 12502  & #standyby2 data node
cd ..

#cd worker4-standby1
#sudo rm -rf log4j2.log
#sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
#java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
#112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
#212.64.64.185 12503 & #primary data node
#cd ..
#
#cd worker4-standby2
#sudo rm -rf log4j2.log
#sudo  find ./ -name 'snapshot*-*' -exec rm {} \;
#java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar Worker  \
#112.124.23.139:2181,112.124.23.139:2182,112.124.23.139:2183 212.64.64.185 12302 \
#212.64.64.185 12504 & #primary data node