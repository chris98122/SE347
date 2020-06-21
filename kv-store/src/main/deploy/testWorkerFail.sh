sleep 20s

sudo netstat -tunlp| grep 12201 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary worker1的端口