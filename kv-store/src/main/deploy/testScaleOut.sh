mkdir worker3
cd worker3
sudo rm -rf log4j2.log
sudo netstat -tunlp| grep 12200 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9
sudo netstat -tunlp| grep 12301 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9
sudo netstat -tunlp| grep 12302 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9
java -cp  ../kv-store/target/kv-store-1.0-SNAPSHOT.jar ScaleOutTest &