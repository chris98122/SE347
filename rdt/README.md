517021911052

周佳懿

chris98122@sjtu.edu.cn

## Design

###### 帧定界

因为是模拟link layer 到physical layer，所以一个frame和一个packet能够对应起来，所以帧定界这一个步骤就被省略了。

###### frame header

包含payload size ，kind, seq, ack ，和checksum 

###### error detection

使用CRC8,生成多项式为g(x)=x8+x5+x4+1

###### packet 的 数据分布

| kind,seq,ack | payload size | payload     | CRC    |
| ------------ | ------------ | ----------- | ------ |
| 1byte        | 1 byte       | 最多125byte | 1 byte |

###### Retransmission





## Implementation

###### timer

keep a chain of virtual timers ordered in their expiration time and the physical timer will go off at the first virtual timer expiration. 

###### Timeout 

0.3 second

###### sender window

STL的unordered_map实现



###### additional buffer



###### Window size 

 a window size of 10



## Performance

###### overhead