package cc.qxt063.blockStorageSimulator.components.search;

import cc.qxt063.blockStorageSimulator.components.blockGenerator.BucketUtils;
import cc.qxt063.blockStorageSimulator.components.bpt.TreeIndex;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.KVPair;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.DataSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;

import java.util.*;

public class MultiSearcher implements Searcher {
    private int blockMax;
    private String multiPath; // /home/sayumi/blockdata/test/multi

    /**
     * 块内索引补全模板
     * /inblock_index/collect-%d-did-%d
     * (height,bucket)
     */
    private String indexPattern_inBlockCollectDid;

    private TreeIndex index_deviceId, index_instId, index_instDid, index_collectId;
    /**
     * 交易位置
     */
    private String txPath_device, txPath_inst, txPath_collect;
    /**
     * 区块位置
     */
    private String bPath_device, bPath_inst, bPath_collect;


    public MultiSearcher(int blockMax, String multiPath) {
        this.blockMax = blockMax;
        this.multiPath = multiPath;
        // init index file
        this.index_deviceId = new TreeIndex(multiPath + "/index_backup/block" + blockMax + "_device_id.index", true, false);
        this.index_instId = new TreeIndex(multiPath + "/index_backup/block" + blockMax + "_inst_id.index", true, false);
        this.index_instDid = new TreeIndex(multiPath + "/index_backup/block" + blockMax + "_inst_did.index", false, false);
        this.index_collectId = new TreeIndex(multiPath + "/index_backup/block" + blockMax + "_collect_id.index", true, false);
        // 块内索引
        this.indexPattern_inBlockCollectDid = multiPath + "/inblock_index/collect-%d-did-%d";
        // 交易路径
        this.txPath_device = multiPath + "/blocktx/device";
        this.txPath_inst = multiPath + "/blocktx/instruction";
        this.txPath_collect = multiPath + "/blocktx/collect";
        // 区块路径
        this.bPath_device = multiPath + "/blockhead/device";
        this.bPath_inst = multiPath + "/blockhead/instruction";
        this.bPath_collect = multiPath + "/blockhead/collect";
    }

    //1. 索引查询等值 -> 查询 instruction.did
    @Override
    public long searchInstByDid(long did) {
        long start = System.currentTimeMillis();
        List<String> pointersStr = index_instDid.findByKey(did);
        if (pointersStr == null) return -1L;
        for (String ptStr : pointersStr) {
            String[] pt = ptStr.split(":");
            DataTransaction tx = JsonUtils.fromJson(FileUtils.readFromFile(
                    txPath_inst + "/" + pt[0] + "/" + pt[1]), DataTransaction.class);
//            System.out.println(tx);
        }
        return System.currentTimeMillis() - start;
    }

    //2. 无索引查询等值 -> 查询 collect.did
    @Override
    public long searchCollectByDid(long did) {
        // TODO: 这里所有的无索引查询（块内索引查询）的blockHeight都是有问题的。传入的blockMax应该为版本链。理论值应该比blockMax小一些
        // Todo not to do 但是这里无所谓，因为每个区块都有这个语义的数据，就当测最差性能了
        //  (人和代码有一个能跑就行)

        long start = System.currentTimeMillis();
        int bucketNum = (int) BucketUtils.getDidBucket(did);
        // 区块从1开始
        for (int blockHeight = blockMax; blockHeight > 0; blockHeight--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blockHeight), Block.class);
            DataSpecial bs = (DataSpecial) b.getHeader().getSpecial();
            // 读取桶索引
            if (bs.getBuckets().get("did").charAt(bucketNum) != '1') continue;
            // 读取块内索引
            TreeIndex index = new TreeIndex(String.format(indexPattern_inBlockCollectDid, blockHeight, bucketNum)
                    , false, false);
            List<String> ptsStr = index.findByKey(did);
            if (ptsStr == null) return -1L;
            for (String pStr : ptsStr) {
                DataTransaction tx = JsonUtils.fromJson(FileUtils.readFromFile(
                        txPath_collect + "/" + blockHeight + "/" + pStr), DataTransaction.class);
//                System.out.println(tx);
            }
        }
        return System.currentTimeMillis() - start;
    }

    //3. 索引查询范围 -> 查询 instruction.did
    @Override
    public long searchRangeInstByDid(long min, long max) {
        long start = System.currentTimeMillis();
        TreeMap<Long, List<String>> ptsStr = index_instDid.findByRange(min, max);
//        return System.currentTimeMillis() - start;
        // 区块IO需要合并，不然IO开销巨大，先返回索引搜索时间，下同
        if (ptsStr == null) return -1L;
        Set<String> blockToRead = new HashSet<>(); // IO合并
        for (Map.Entry<Long, List<String>> ptsEntry : ptsStr.entrySet()) {
            for (String ptStr : ptsEntry.getValue()) {
                String[] pt = ptStr.split(":");
                blockToRead.add(pt[0]); //IO合并
//                DataTransaction tx = JsonUtils.fromJson(FileUtils.readFromFile(
//                        txPath_inst + "/" + pt[0] + "/" + pt[1]), DataTransaction.class);
//                System.out.println(tx);
            }
        }
        // 区块IO
        for (String blkHeight : blockToRead) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_inst + "/" + blkHeight), Block.class);
        }

        return System.currentTimeMillis() - start;
    }

    //3. 无IO索引查询范围 -> 查询 instruction.did
    public long searchRangeInstByDid_noIO(long min, long max) {
        long start = System.currentTimeMillis();
        TreeMap<Long, List<String>> ptsStr = index_instDid.findByRange(min, max);
        return System.currentTimeMillis() - start;
    }

    //4. 无索引查询范围 -> 查询 collect.did
    @Override
    public long searchRangeCollectByDid(long min, long max) {
        long start = System.currentTimeMillis();
        int bucketNumMin = (int) BucketUtils.getDidBucket(min);
        int bucketNumMax = (int) BucketUtils.getDidBucket(max);
        Set<Integer> blockToRead = new HashSet<>(); // IO合并
        // 区块从1开始
        for (int blockHeight = blockMax; blockHeight > 0; blockHeight--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blockHeight), Block.class);
            DataSpecial bs = (DataSpecial) b.getHeader().getSpecial();
            // 遍历桶，读取桶索引
            for (int bucketNum = bucketNumMin; bucketNum <= bucketNumMax; bucketNum++) {
                if (bs.getBuckets().get("did").charAt(bucketNum) != '1') continue;
                // 读取块内索引
                TreeIndex index = new TreeIndex(String.format(indexPattern_inBlockCollectDid, blockHeight, bucketNum)
                        , false, false);
                // 范围搜索，替换范围
                long[] range = BucketUtils.getDidBucketRange(bucketNum);
                if (bucketNum == bucketNumMin) range[0] = min;
                if (bucketNum == bucketNumMax) range[1] = max;

                TreeMap<Long, List<String>> ptsStr = index.findByRange(min, max);
                if (ptsStr == null) return -1L;
                blockToRead.add(blockHeight); // 合并IO
//                for (Map.Entry<Long, List<String>> ptsEntry : ptsStr.entrySet()) {
//                    for (String ptStr : ptsEntry.getValue()) {
//                        DataTransaction tx = JsonUtils.fromJson(FileUtils.readFromFile(
//                                txPath_collect + "/" + blockHeight + "/" + ptStr), DataTransaction.class);
////                        System.out.println(tx);
//                    }
//                }
            }
        }

        // 区块IO
        for (Integer blkHeight : blockToRead) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blkHeight), Block.class);
        }
        return System.currentTimeMillis() - start;
    }


    //4. 无IO无索引查询范围 -> 查询 collect.did
    public long searchRangeCollectByDid_noIO(long min, long max) {
        long start = System.currentTimeMillis();
        int bucketNumMin = (int) BucketUtils.getDidBucket(min);
        int bucketNumMax = (int) BucketUtils.getDidBucket(max);
        // 区块从1开始
        for (int blockHeight = blockMax; blockHeight > 0; blockHeight--) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blockHeight), Block.class);
            DataSpecial bs = (DataSpecial) b.getHeader().getSpecial();
            // 遍历桶，读取桶索引
            for (int bucketNum = bucketNumMin; bucketNum <= bucketNumMax; bucketNum++) {
                if (bs.getBuckets().get("did").charAt(bucketNum) != '1') continue;
                // 读取块内索引
                TreeIndex index = new TreeIndex(String.format(indexPattern_inBlockCollectDid, blockHeight, bucketNum)
                        , false, false);
                // 范围搜索，替换范围
                long[] range = BucketUtils.getDidBucketRange(bucketNum);
                if (bucketNum == bucketNumMin) range[0] = min;
                if (bucketNum == bucketNumMax) range[1] = max;

                TreeMap<Long, List<String>> ptsStr = index.findByRange(min, max);
                if (ptsStr == null) return -1L;
            }
        }
        return System.currentTimeMillis() - start;
    }

    //5. 双索引连接 -> 查询 device.did = instruction.did
    @Override
    public long searchDeviceDidEqualsInstDid() {
        long start = System.currentTimeMillis();
        Iterator<List<KVPair>> ddidIt = index_deviceId.iterator(), ididIt = index_instDid.iterator();
        if (!ddidIt.hasNext() || !ididIt.hasNext()) {
            return -1;
        }
        List<KVPair> d = ddidIt.next(), i = ididIt.next();
        while (ddidIt.hasNext() && ididIt.hasNext()) {
            while (d.get(0).getK() < i.get(0).getK()) d = ddidIt.next();
            while (i.get(0).getK() < d.get(0).getK()) i = ididIt.next();
            // 相等
            if (d.get(0).getK() == i.get(0).getK()) {
                for (KVPair dkv : d) {
                    String[] dpt = dkv.getV().split(":");
                    DataTransaction dtx = JsonUtils.fromJson(FileUtils.readFromFile(
                            txPath_device + "/" + dpt[0] + "/" + dpt[1]), DataTransaction.class);
                    for (KVPair ikv : i) {
                        String[] ipt = ikv.getV().split(":");
                        DataTransaction itx = JsonUtils.fromJson(FileUtils.readFromFile(
                                txPath_inst + "/" + ipt[0] + "/" + ipt[1]), DataTransaction.class);
                        // connect
//                        System.out.println(dtx.getAttributeData().get("did") + " : " + itx.getAttributeData().get("did") +
//                                "\tat inst block\t" + ipt[0]);
                    }
                }
            }
            if (ddidIt.hasNext()) d = ddidIt.next();
            if (ididIt.hasNext()) i = ididIt.next();
        }
        return System.currentTimeMillis() - start;
    }

    //6. 索引-非索引连接(仅查询：构建哈希；验证：块内索引) -> 查询 device.did = collect.did
    @Override
    public long searchDeviceDidEqualsCollectDid() {
        long allTime = System.currentTimeMillis();

//        long readBlockHeadTime = System.currentTimeMillis();
        // 区块从1开始，读取出所有的区块bucket
        List<String> buckets = new ArrayList<>(blockMax + 5);
        for (int blockHeight = 1; blockHeight <= blockMax; blockHeight++) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blockHeight), Block.class);
            DataSpecial bs = (DataSpecial) b.getHeader().getSpecial();
            buckets.add(bs.getBuckets().get("did"));
        }
//        allTime += System.currentTimeMillis() - readBlockHeadTime;


        // 有索引的只用遍历一遍
        Iterator<List<KVPair>> ddidIt = index_deviceId.iterator();
        List<KVPair> d = ddidIt.next();
        // 遍历桶，读取桶索引
        for (int bucketNum = 0; bucketNum <= 9; bucketNum++) {
//            List<TreeIndex> indexes = new ArrayList<>();
//            for (int blockHeight_1 = 0; blockHeight_1 < buckets.size(); blockHeight_1++) {
//                String bucket = buckets.get(blockHeight_1);
//                if (bucket.charAt(bucketNum) != '1') indexes.add(null);
//                else indexes.add(new TreeIndex(String.format(
//                        indexPattern_inBlockCollectDid, blockHeight_1 + 1, bucketNum), false, false));
//            }
//
////            // 所有索引的迭代器，可能null
//            List<Iterator<List<KVPair>>> iters = indexes.stream().map(x -> x == null ? null : x.iterator()).collect(Collectors.toList());
//            // 初始化迭代元素
//            List<List<KVPair>> is = new ArrayList<>();
//            for (Iterator<List<KVPair>> iter : iters) {
//                if (iter != null && iter.hasNext()) is.add(iter.next());
////                else is.add(null);
//            }

            // 索引内哈希连接
            Map<Long, List<String>> map = new HashMap<>();
            for (int blockHeight_1 = 0; blockHeight_1 < buckets.size(); blockHeight_1++) {
                String bucket = buckets.get(blockHeight_1);
                if (bucket.charAt(bucketNum) != '1')
                    continue;
//                System.out.println("load\t" + blockHeight_1 + 1 + ":" + bucketNum);
                TreeIndex index = new TreeIndex(String.format(
                        indexPattern_inBlockCollectDid, blockHeight_1 + 1, bucketNum), false, false);
                for (Map.Entry<Long, List<String>> e : index.getAllNodesWithHeight(blockHeight_1 + 1).entrySet()) {
                    if (!map.containsKey(e.getKey())) map.put(e.getKey(), e.getValue());
                    else map.get(e.getKey()).addAll(e.getValue());
                }
                index.close();
            }
            while (ddidIt.hasNext() && d.get(0).getK() < BucketUtils.getDidBucketRange(bucketNum)[1]) {
                long k = d.get(0).getK();
                if (map.containsKey(k)) {
                    for (KVPair dkv : d) {
                        String[] dpt = dkv.getV().split(":");
                        DataTransaction dtx = JsonUtils.fromJson(FileUtils.readFromFile(
                                txPath_device + "/" + dpt[0] + "/" + dpt[1]), DataTransaction.class);
                        for (String cptWithH : map.get(k)) {
                            String[] cpt = cptWithH.split(":");
                            DataTransaction ctx = JsonUtils.fromJson(FileUtils.readFromFile(
                                    txPath_collect + "/" + cpt[0] + "/" + cpt[1]), DataTransaction.class);
                            // connect
//                            System.out.println(dtx.getAttributeData().get("did") + " : " + ctx.getAttributeData().get("did") +
//                                    "\tat collect block\t" + cpt[0]);
                        }
                    }
                }
                d = ddidIt.next();
            }
        }


        // 区块从1开始
//        for (int blockHeight = FileUtils.getFileCount(bPath_collect); blockHeight > 0; blockHeight--) {
//
//            // 有索引的只用遍历一遍
//            Iterator<List<KVPair>> ddidIt = index_deviceId.iterator();
//            // 遍历桶，读取桶索引
//            for (int bucketNum = 0; bucketNum <= 9; bucketNum++) {
//                // 遍历索引为bucketNum的桶的所有区块
//
//                if (bs.getBuckets().get("did").charAt(bucketNum) != '1') continue;
//                // 读取块内索引
//                TreeIndex index = new TreeIndex(String.format(indexPattern_inBlockCollectDid, blockHeight, bucketNum)
//                        , false, false);
//                Iterator<List<KVPair>> iit = index.iterator();
//                if (!ddidIt.hasNext() || !iit.hasNext()) {
//                    continue;
//                }
//
//                long searchTime = System.currentTimeMillis();
//                List<KVPair> d = ddidIt.next(), i = iit.next();
//                while (ddidIt.hasNext() && iit.hasNext()) {
//                    while (d.get(0).getK() < i.get(0).getK()) d = ddidIt.next();
//                    while (i.get(0).getK() < d.get(0).getK()) i = iit.next();
//                    // 相等
//                    if (d.get(0).getK() == i.get(0).getK()) {
//                        for (KVPair dkv : d) {
//                            String[] dpt = dkv.getV().split(":");
//                            DataTransaction dtx = JsonUtils.fromJson(FileUtils.readFromFile(
//                                    txPath_device + "/" + dpt[0] + "/" + dpt[1]), DataTransaction.class);
//                            for (KVPair ikv : i) {
//                                String ipt = ikv.getV();
//                                DataTransaction itx = JsonUtils.fromJson(FileUtils.readFromFile(
//                                        txPath_collect + "/" + blockHeight + "/" + ipt), DataTransaction.class);
//                                // connect
//                                System.out.println(dtx.getAttributeData().get("did") + " : " + itx.getAttributeData().get("did") +
//                                        "\tat block " + blockHeight);
//                            }
//                        }
//                    }
//                    if (ddidIt.hasNext()) d = ddidIt.next();
//                    if (iit.hasNext()) i = iit.next();
//                }
//                allTime += System.currentTimeMillis() - searchTime;
//            }
//        }
        return System.currentTimeMillis() - allTime;
    }

    //7. 双非索引连接(仅查询：构建哈希；验证：块内索引) -> 查询 collect.did = collect.did
    @Override
    public long searchCollectDidEqualsCollectDid() {
        long start = System.currentTimeMillis();
        // 哈希连接，遍历区块按桶进行连接
        List<String> buckets = new ArrayList<>(blockMax + 5);
        for (int blockHeight = 1; blockHeight <= blockMax; blockHeight++) {
            Block b = JsonUtils.fromJson(FileUtils.readFromFile(bPath_collect + "/" + blockHeight), Block.class);
            DataSpecial bs = (DataSpecial) b.getHeader().getSpecial();
            buckets.add(bs.getBuckets().get("did"));
        }

        for (int bucketNum = 0; bucketNum <= 9; bucketNum++) {
            Map<Long, List<String>> m1 = new HashMap<>();
            for (int blockHeight_1 = 0; blockHeight_1 < buckets.size(); blockHeight_1++) {
                String bucket = buckets.get(blockHeight_1);
                if (bucket.charAt(bucketNum) != '1')
                    continue;
//                System.out.println("load\t" + blockHeight_1 + 1 + ":" + bucketNum);
                TreeIndex index = new TreeIndex(String.format(
                        indexPattern_inBlockCollectDid, blockHeight_1 + 1, bucketNum), false, false);
                for (Map.Entry<Long, List<String>> e : index.getAllNodesWithHeight(blockHeight_1 + 1).entrySet()) {
                    if (!m1.containsKey(e.getKey())) m1.put(e.getKey(), e.getValue());
                    else m1.get(e.getKey()).addAll(e.getValue());
                }
                index.close();
            }
            // 连接 同一个桶
            for (int blockHeight_1 = 0; blockHeight_1 < buckets.size(); blockHeight_1++) {
                String bucket = buckets.get(blockHeight_1);
                if (bucket.charAt(bucketNum) != '1')
                    continue;
                TreeIndex index = new TreeIndex(String.format(
                        indexPattern_inBlockCollectDid, blockHeight_1 + 1, bucketNum), false, false);
                for (Map.Entry<Long, List<String>> e : index.getAllNodesWithHeight(blockHeight_1 + 1).entrySet()) {
                    if (m1.containsKey(e.getKey())) {
                        // connect
                        for (String c1 : m1.get(e.getKey())) {
                            String[] cpt1 = c1.split(":");
                            for (String c2 : e.getValue()) {
                                String[] cpt2 = c2.split(":");
                                // 连接spt1和spt2
                                DataTransaction ctx1 = JsonUtils.fromJson(FileUtils.readFromFile(
                                        txPath_collect + "/" + cpt1[0] + "/" + cpt1[1]), DataTransaction.class);
                                DataTransaction ctx2 = JsonUtils.fromJson(FileUtils.readFromFile(
                                        txPath_collect + "/" + cpt2[0] + "/" + cpt2[1]), DataTransaction.class);
//                                System.out.println(ctx1.getAttributeData().get("did") + "\tat collect 1 block\t" + cpt1[0]);
                            }
                        }
                    }
                }
                index.close();
            }

        }

        return System.currentTimeMillis() - start;
    }

}
