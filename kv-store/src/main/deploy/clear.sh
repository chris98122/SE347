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
