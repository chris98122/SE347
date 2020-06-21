sleep 1m #先往primary  data 插入数据

sudo netstat -tunlp| grep 12201 | awk  '{print $7}' |cut -d"/" -f1 |sudo xargs kill -9 #primary worker1的端口