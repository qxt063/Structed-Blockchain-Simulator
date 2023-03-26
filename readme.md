# 区块链存储模拟
## 语义结构
device(
did:int[1,10000]:pk,
dname:string,
ddesp:string,
dkey:string
)

设备（设备id，设备名称，设备描述，设备公钥）

instruction(
iid:int:pk,
did:int[1,10000]:index,
idata:string,
ifrom:string
)

指令（指令id，设备id，指令数据，发送人）

collect_data(
cid:int:pk,
did:int[1,10000],
cdata:string,
csign:string
)

采集数据（数据id，设备id，数据内容，签名）

## 测试功能
1. 索引查询等值
2. 无索引查询等值
3. 索引查询范围
4. 无索引查询范围
5. 双索引连接
6. 索引-非索引连接
7. 双非索引连接

## 测试数据
bucket = 10;
