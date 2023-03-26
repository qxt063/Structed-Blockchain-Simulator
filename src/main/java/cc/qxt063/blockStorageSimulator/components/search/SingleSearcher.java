package cc.qxt063.blockStorageSimulator.components.search;

import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleSearcher implements Searcher {
    private int blockMax;
    private String singlePath, blockPath;

    public SingleSearcher(int height, String singlePath) {
        this.blockMax = height;
        this.singlePath = singlePath;
        this.blockPath = this.singlePath + "/block";
    }

    @Override
    public long searchInstByDid(long did) {
        long start = System.currentTimeMillis();
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("iid") &&
                        Long.parseLong(dtx.getAttributeData().get("did")) == did) {
                    // 找到
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long searchCollectByDid(long did) {
        long start = System.currentTimeMillis();
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("cid") &&
                        Long.parseLong(dtx.getAttributeData().get("did")) == did) {
                    // 找到
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long searchRangeInstByDid(long min, long max) {
        long start = System.currentTimeMillis();
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("iid") &&
                        Long.parseLong(dtx.getAttributeData().get("did")) >= min &&
                        Long.parseLong(dtx.getAttributeData().get("did")) <= max) {
                    // 找到
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long searchRangeCollectByDid(long min, long max) {
        long start = System.currentTimeMillis();
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("cid") &&
                        Long.parseLong(dtx.getAttributeData().get("did")) >= min &&
                        Long.parseLong(dtx.getAttributeData().get("did")) <= max) {
                    // 找到
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long searchDeviceDidEqualsInstDid() {
        long start = System.currentTimeMillis();
        // 哈希索引连接
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            // 查找所有属于device的交易，存入hashmap
            Map<Long, DataTransaction> deviceTrans = new HashMap<>();
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("dname"))
                    deviceTrans.put(Long.parseLong(dtx.getAttributeData().get("did")), dtx);
            }
            // 没有，继续搜下一个
            if (deviceTrans.size() == 0) continue;
            // 嵌套搜索区块，哈希连接
            for (int j = blockMax; j > 0; j--) {
                Block b2 = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + j), Block.class);
                for (Transaction tx : b2.getBody()) {
                    DataTransaction dtx = (DataTransaction) tx;
                    if (dtx.getAttributeData().containsKey("iid") &&
                            deviceTrans.containsKey(Long.parseLong(dtx.getAttributeData().get("did")))) {
                        // 找到
                    }
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long searchDeviceDidEqualsCollectDid() {
        long start = System.currentTimeMillis();
        // 哈希索引连接
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            // 查找所有属于device的交易，存入hashmap
            Map<Long, DataTransaction> deviceTrans = new HashMap<>();
            for (Transaction tx : b.getBody()) {
                DataTransaction dtx = (DataTransaction) tx;
                if (dtx.getAttributeData().containsKey("dname"))
                    deviceTrans.put(Long.parseLong(dtx.getAttributeData().get("did")), dtx);
            }
            // 没有，继续搜下一个
            if (deviceTrans.size() == 0) continue;
            // 嵌套搜索区块，哈希连接
            for (int j = blockMax; j > 0; j--) {
                Block b2 = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + j), Block.class);
                for (Transaction tx : b2.getBody()) {
                    DataTransaction dtx = (DataTransaction) tx;
                    if (dtx.getAttributeData().containsKey("cid") &&
                            deviceTrans.containsKey(Long.parseLong(dtx.getAttributeData().get("did")))) {
                        // 找到
                    }
                }
            }
        }
        return System.currentTimeMillis() - start;
    }

    // 虽然是查询同一语义，但是通常是不同语义结构，因此需要全部搜索而不是递减搜索
    @Override
    public long searchCollectDidEqualsCollectDid() {
        long start = System.currentTimeMillis();
        // 哈希索引连接
        for (int i = blockMax; i > 0; i--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + i), Block.class);
            // 查找所有属于collect_data的交易，存入hashmap
            Map<Long, List<DataTransaction>> collectTrans = new HashMap<>();
            for (Transaction tx : b.getBody()) {
                DataTransaction ctx = (DataTransaction) tx;
                if (ctx.getAttributeData().containsKey("cid")) {
                    long did = Long.parseLong(ctx.getAttributeData().get("did"));
                    if (!collectTrans.containsKey(did))
                        collectTrans.put(did, new ArrayList<>());
                    collectTrans.get(did).add(ctx);
                }

            }
            // 没有，继续搜下一个
            if (collectTrans.size() == 0) continue;
            // 嵌套搜索区块，哈希连接
            for (int j = blockMax; j > 0; j--) {
                Block b2 = JsonUtils.fromJson(FileUtils.readFromFile(blockPath + "/" + j), Block.class);
                for (Transaction tx : b2.getBody()) {
                    DataTransaction ctx = (DataTransaction) tx;
                    if (ctx.getAttributeData().containsKey("cid") &&
                            collectTrans.containsKey(Long.parseLong(ctx.getAttributeData().get("did")))) {
                        // 找到
                    }
                }
            }
        }
        return System.currentTimeMillis() - start;
    }
}
