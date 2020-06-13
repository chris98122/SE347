mkdir worker1
mkdir worker2
cd worker1
java -cp  ./kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181 212.64.64.185 12200 &
cd ..
cd worker2
java -cp  ./kv-store-1.0-SNAPSHOT.jar Worker  112.124.23.139:2181 212.64.64.185 12201 &