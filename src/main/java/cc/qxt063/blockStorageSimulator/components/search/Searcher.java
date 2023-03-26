package cc.qxt063.blockStorageSimulator.components.search;

public interface Searcher {
    //## 测试功能
    //1. 索引查询等值 -> 查询 instruction.did
    //2. 无索引查询等值 -> 查询 collect.did
    //3. 索引查询范围 -> 查询 instruction.did
    //4. 无索引查询范围 -> 查询 collect.did
    //5. 双索引连接 -> 查询 device.did = instruction.did
    //6. 索引-非索引连接(仅查询：构建哈希；验证：块内索引) -> 查询 device.did = collect.did
    //7. 双非索引连接(仅查询：构建哈希；验证：块内索引) -> 查询 collect.did = collect.did
    // 对于每种查询，查询100，200 ... 1000区块(块内交易500，1000，2000，4000分别测)下的时间

    /**
     * instruction.did
     */
    long searchInstByDid(long did);

    /**
     * collect.did
     */
    long searchCollectByDid(long did);

    /**
     * range instruction.did
     */
    long searchRangeInstByDid(long min, long max);

    /**
     * range  collect.did
     */
    long searchRangeCollectByDid(long min, long max);

    /**
     * device.did = instruction.did
     */
    long searchDeviceDidEqualsInstDid();

    /**
     * device.did = collect.did
     */
    long searchDeviceDidEqualsCollectDid();

    /**
     * collect.did = collect.did
     */
    long searchCollectDidEqualsCollectDid();

}
