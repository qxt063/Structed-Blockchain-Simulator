package cc.qxt063.blockStorageSimulator.components.blockGenerator;

import cc.qxt063.blockStorageSimulator.components.bpt.TreeIndex;
import cc.qxt063.blockStorageSimulator.components.timeCounter.MultiBuildTimeCounter;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockHeader;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockMeta;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.DataSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.*;

import java.util.*;

/**
 * 原始交易数据转为多链，且创建索引
 */
public class Raw2MultiConverter {
    /*
        跨区块索引有如下：
        pk_index: device: did; instruction: iid; collect:cid;
        attr_index: instruction: did;
        ===========================================================
        pk:
        device_did, iid, cid

        attr:
        inst_did

        块内索引命名方式：（由于只以did作为键查询测试，仅考虑collect_did属性上的索引）
        height_semantics_attr_bucket
     */

    /*
        1. 读取所有交易，划分到三条交易链（文件夹模拟块内随机访问）。插入时间测试仍计算整个区块后写入一个文件。
        2. 计算pk和attr index
        3. 对每条交易链，计算块内索引。

        注：每50个区块保存一次索引文件(pk和attr)
        Block写入MULTI_BLOCK，交易复制到MULTI_RANDOM_TX
     */

    /*
        出块时间分析:
        对于每一个区块：
        1. 属性索引插入时间
        2. 属性索引merkle计算时间(pages次)
        3. 块内索引插入时间
        4. 块内索引merkle计算时间(pages次)
        5. 交易merkle计算时间
     */

    /**
     * 测试用哈希值
     */
    private String testHash = "d8089756853ac93ccd84dddfca710dea5072c04e543c2c3c67eafd849d5017c0";

    /**
     * 目标文件夹
     */
    private String blockHeadFolder, blockTxFolder, attrIndexFolder, inBlockIndexFolder, attrIndexBackupFolder;
    /**
     * index路径
     */
    private String deviceIdIndexPath, instIdIndexPath, collectIdIndexPath, instDidIndexPath;
    /**
     * index备份路径模板
     */
    private String deviceDidIndexBackupPattern, instIdIndexBackupPattern, collectIdIndexBackupPattern, instDidIndexBackupPattern;
    private int indexPageSize = 1024;
    /**
     * index
     */
    private TreeIndex deviceIdIndex, instIdIndex, collectIdIndex, instDidIndex;

//    /**
//     * 建立B+树属性索引的阈值
//     */
//    private Long threshold = 100L;
    /**
     * 计数器
     */
    private MultiBuildTimeCounter timer;
    /**
     * 属性索引插入时间
     */
    Long attrInsertOnce = 0L;
    /**
     * 属性索引merkle计算时间
     */
    Long attrMerkleOnce = 0L;
    /**
     * 块内索引插入时间
     */
    Long inBlockInsertOnce = 0L;
    /**
     * 块内索引merkle计算时间
     */
    Long inBlockMerkleOnce = 0L;
    /**
     * 交易merkle计算时间
     */
    Long txMerkleOnce = 0L;
    /**
     * 总时间
     */
    Long allTimeOnce = 0L;
    /**
     * 复制交易的IO操作耗时，最后减掉
     */
    Long ioTimeOnce = 0L;

    /**
     * 每过该数值备份一次索引
     */
    private int indexBackupNum;


    public Raw2MultiConverter(String multiFolder, int indexBackupNum) {
        initFolder(multiFolder);
        this.indexBackupNum = indexBackupNum;
        // 初始化跨块属性索引
        deviceIdIndex = new TreeIndex(deviceIdIndexPath, true, this.indexPageSize, true);
        instIdIndex = new TreeIndex(instIdIndexPath, true, this.indexPageSize, true);
        collectIdIndex = new TreeIndex(collectIdIndexPath, true, this.indexPageSize, true);
        instDidIndex = new TreeIndex(instDidIndexPath, false, this.indexPageSize, true);
        // 初始化计数器
        timer = new MultiBuildTimeCounter(multiFolder + "/time.csv");
    }


    /**
     * 生成链簇区块
     *
     * @param rawFolder
     */
    public void genMultiBlocksFromRaw(String rawFolder) {
        Long dh = 1L, ih = 1L, ch = 1L;
        String dph = "0000000000000000000000000000000000000000000000000000000000000000",
                iph = "0000000000000000000000000000000000000000000000000000000000000000",
                cph = "0000000000000000000000000000000000000000000000000000000000000000";

        for (int i = 0; i < FileUtils.getFolderCount(rawFolder); i++) {
            long allTimeTmp = System.currentTimeMillis();

            String oneRawBlockFolder = rawFolder + "/" + i;
            List<Block> blocks = readOneRawBlock(dh, dph, ih, iph, ch, cph, oneRawBlockFolder, blockTxFolder);
            Block db = blocks.get(0), ib = blocks.get(1), cb = blocks.get(2);
            if (db != null) dph = HashUtils.sha256Hex(db);
            iph = HashUtils.sha256Hex(ib);
            cph = HashUtils.sha256Hex(cb);


            // 写入区块数据
            if (db != null) {
                db.setBody(null);
                FileUtils.writeToFile(blockHeadFolder + "/device/" + dh, db.toString());
            }
            ib.setBody(null);
            FileUtils.writeToFile(blockHeadFolder + "/instruction/" + ih, ib.toString());
            cb.setBody(null);
            FileUtils.writeToFile(blockHeadFolder + "/collect/" + ch, cb.toString());

//            System.out.println("=========== rawID: " + i + " ===========");
//            System.out.printf("%s: %d-%s\n", "device", dh, dph);
//            System.out.printf("%s: %d-%s\n", "instruction", ih, iph);
//            System.out.printf("%s: %d-%s\n", "collect", ch, cph);

            allTimeOnce += System.currentTimeMillis() - allTimeTmp;
//            timer.count(attrInsertOnce, attrMerkleOnce, inBlockInsertOnce, inBlockMerkleOnce, txMerkleOnce, allTimeOnce - ioTimeOnce);
            timer.count(attrInsertOnce, attrMerkleOnce, inBlockInsertOnce, inBlockMerkleOnce, txMerkleOnce, ioTimeOnce, allTimeOnce);

            if (db != null) dh++;
            ih++;
            ch++;

            if ((i + 1) % indexBackupNum == 0) { // 索引备份每x个区块备份一次索引
                deviceIdIndex.backupIndex(String.format(deviceDidIndexBackupPattern, i + 1));
                instIdIndex.backupIndex(String.format(instIdIndexBackupPattern, i + 1));
                collectIdIndex.backupIndex(String.format(collectIdIndexBackupPattern, i + 1));
                instDidIndex.backupIndex(String.format(instDidIndexBackupPattern, i + 1));
            }
            attrInsertOnce = attrMerkleOnce = inBlockInsertOnce = inBlockMerkleOnce = txMerkleOnce = allTimeOnce = ioTimeOnce = 0L;
        }
        timer.saveCsv();
    }


    /**
     * 从文件夹读取一个Raw区块，并build为链簇
     */
    @SuppressWarnings("DuplicatedCode")
    private List<Block> readOneRawBlock(Long dHeight, String dPreHash,
                                        Long iHeight, String iPreHash,
                                        Long cHeight, String cPreHash,
                                        String rawBlockFolder, String destBlockTxFolder) {
        long ioTmp = System.currentTimeMillis();
        ArrayList<Transaction> devList = new ArrayList<>(), instList = new ArrayList<>(), colleList = new ArrayList<>();
        String dPath = rawBlockFolder + "/device", iPath = rawBlockFolder + "/instruction", cPath = rawBlockFolder + "/collect";
        int dNum = FileUtils.getFileCount(dPath);
        int iNum = FileUtils.getFileCount(iPath);
        int cNum = FileUtils.getFileCount(cPath);
        // read device
        for (int i = 0; i < dNum; i++) {
            devList.add(JsonUtils.fromJson(FileUtils.readFromFile(dPath + '/' + i), DataTransaction.class));
        }
        for (int i = 0; i < iNum; i++) {
            instList.add(JsonUtils.fromJson(FileUtils.readFromFile(iPath + '/' + i), DataTransaction.class));
        }
        for (int i = 0; i < cNum; i++) {
            colleList.add(JsonUtils.fromJson(FileUtils.readFromFile(cPath + '/' + i), DataTransaction.class));
        }
        ioTimeOnce += System.currentTimeMillis() - ioTmp;

        // 对三类交易，依次buildBlock，将区块写入blockHeader
        List<Block> res = new ArrayList<>();
        if (devList.size() > 0)
            res.add(buildBlock(dHeight, dPreHash, devList, "data", "device", dPath, destBlockTxFolder + "/device"));
        else res.add(null);
        res.add(buildBlock(iHeight, iPreHash, instList, "data", "instruction", iPath, destBlockTxFolder + "/instruction"));
        res.add(buildBlock(cHeight, cPreHash, colleList, "data", "collect", cPath, destBlockTxFolder + "/collect"));
        return res;
    }


    /**
     * 存储某一类交易，创建索引，计算merkle
     */
    private Block buildBlock(Long height, String preHash, ArrayList<Transaction> trans, String type, String semantics,
                             String rawTxPath, String destTxPathWithoutHeight) {
        Block b = new Block();
        trans.sort((x, y) -> x.getTimestamp().compareTo(y.timestamp));

        // 计算merkle
        long merkleTmp = System.currentTimeMillis();
        String merkleRoot = MerkleUtils.buildMerkleHex(trans);
        txMerkleOnce += System.currentTimeMillis() - merkleTmp;

        BlockHeader header = new BlockHeader();
        header.setMeta(new BlockMeta(height, type, System.currentTimeMillis(), preHash, merkleRoot));

        // build index
        // build block header
        BlockSpecial bs = null;
        if ("data".equals(type)) {
            switch (semantics) {
                case "device":
                    bs = new DataSpecial("device", testHash, testHash,
                            buildDeviceIndex(height, trans), new TreeMap<>());
                    break;
                case "instruction":
                    bs = new DataSpecial("instruction", testHash, testHash,
                            buildInstructionIndex(height, trans), new TreeMap<>());
                    break;
                case "collect":
                    bs = new DataSpecial("collect", testHash, testHash,
                            buildCollectIndex(height, trans), new TreeMap<>(Map.of("did", testHash)));
                    break;
            }
            // 复制区块交易，模拟随机读取
            long ioTmp = System.currentTimeMillis();
            FileUtils.copyFolder(rawTxPath, destTxPathWithoutHeight + "/" + height, false);
            ioTimeOnce += System.currentTimeMillis() - ioTmp;
        }
        header.setSpecial(bs);
        b.setHeader(header);
        b.setBody(trans);
        return b;
    }

    /**
     * 构造索引
     *
     * @param devList
     * @return 块内桶索引
     */
    private TreeMap<String, String> buildDeviceIndex(Long height, List<Transaction> devList) {
        // 属性索引：device_did
        long attrInsertTmp = System.currentTimeMillis();
        for (int i = 0; i < devList.size(); i++) {
            DataTransaction dTx = (DataTransaction) devList.get(i);
            deviceIdIndex.insert(Long.valueOf(dTx.getAttributeData().get("did")), String.format("%s:%d", height, i));
        }
        attrInsertOnce += System.currentTimeMillis() - attrInsertTmp;

        long attrMerkleTmp = System.currentTimeMillis();
        // 计算属性索引merkle
        for (int i = 0; i < deviceIdIndex.getPageSize(); i++) {
            MerkleUtils.waitCalculateMerkle();
        }
        attrMerkleOnce += System.currentTimeMillis() - attrMerkleTmp;

        // 块内索引：null
        return new TreeMap<>();
    }

    private TreeMap<String, String> buildInstructionIndex(Long height, List<Transaction> instList) {
        // 属性索引：iid inst_did
        var attrInsertTmp = System.currentTimeMillis();
        for (int i = 0; i < instList.size(); i++) {
            DataTransaction iTx = (DataTransaction) instList.get(i);
            instIdIndex.insert(Long.valueOf(iTx.getAttributeData().get("iid")), String.format("%s:%d", height, i));
            instDidIndex.insert(Long.valueOf(iTx.getAttributeData().get("did")), String.format("%s:%d", height, i));
        }
        attrInsertOnce += System.currentTimeMillis() - attrInsertTmp;

        long attrMerkleTmp = System.currentTimeMillis();
        // 计算属性索引merkle
        for (int i = 0; i < instIdIndex.getPageSize() + instDidIndex.getPageSize(); i++) {
            MerkleUtils.waitCalculateMerkle();
        }
        attrMerkleOnce += System.currentTimeMillis() - attrMerkleTmp;

        // 块内索引：null
        return new TreeMap<>();
    }

    private TreeMap<String, String> buildCollectIndex(Long height, List<Transaction> collecList) {
        // 属性索引：cid
        long attrInsertTmp = System.currentTimeMillis();
        for (int i = 0; i < collecList.size(); i++) {
            DataTransaction dTx = (DataTransaction) collecList.get(i);
            collectIdIndex.insert(Long.valueOf(dTx.getAttributeData().get("cid")), String.format("%s:%d", height, i));
        }
        attrInsertOnce += System.currentTimeMillis() - attrInsertTmp;

        long attrMerkleTmp = System.currentTimeMillis();
        // 计算属性索引merkle
        for (int i = 0; i < collectIdIndex.getPageSize(); i++) {
            MerkleUtils.waitCalculateMerkle();
        }
        attrMerkleOnce += System.currentTimeMillis() - attrMerkleTmp;

        // 块内索引：did
        // 块内索引命名方式：（由于只以did作为键查询测试，仅考虑collect_did属性上的索引）
        // semantics-height-attr-bucket
        // collect-{height}-did-{bucket}

        // 构造区块内索引的时间
        long inBlockInsertTmp = System.currentTimeMillis();
//       map(bucket, pair<offset,tx>)
        // group by 带偏移的交易
        Map<Long, List<Pair<Long, DataTransaction>>> buckedTrans = new TreeMap<>();
        for (long i = 0; i < collecList.size(); i++) {
            DataTransaction tx = (DataTransaction) collecList.get((int) i);
            Pair<Long, DataTransaction> offsetTx = new Pair<>(i, tx);
            long bucketNum = BucketUtils.getDidBucket(Long.parseLong(tx.getAttributeData().get("did")));
            if (!buckedTrans.containsKey(bucketNum)) buckedTrans.put(bucketNum, new ArrayList<>());
            buckedTrans.get(bucketNum).add(offsetTx);
        }


        TreeMap<String, String> buckets = new TreeMap<>();
        StringBuilder didBucket = new StringBuilder();
        Long hashNum = 0L; // 需要计算的哈希数量
        for (long i = 0L; i < 10; i++) {
            // 遍历桶
            if (!buckedTrans.containsKey(i)) {
                didBucket.append('0');
                continue;
            }
            didBucket.append('1');
            // build inBLock index for bucket i
            TreeIndex bi = new TreeIndex(
                    inBlockIndexFolder + String.format("/collect-%d-did-%d", height, i), false, true);
            // 把桶中的交易放入索引
            for (Pair<Long, DataTransaction> offsetPair : buckedTrans.get(i)) {
                bi.insert(Long.valueOf(offsetPair.getValue().getAttributeData().get("did")), offsetPair.getKey().toString());
            }
            hashNum += bi.getPageSize();
            bi.close();
        }
        // 把桶索引放入Map
        buckets.put("did", didBucket.toString());

        // 构造区块内索引的时间
        inBlockInsertOnce += System.currentTimeMillis() - inBlockInsertTmp;
        // 计算区块内merkle的时间
        hashNum += 10; //bucket hash
        long inBlockMerkleTmp = System.currentTimeMillis();
        for (int i = 0; i < hashNum; i++)
            MerkleUtils.waitCalculateMerkle();
        inBlockMerkleOnce += System.currentTimeMillis() - inBlockMerkleTmp;

        return buckets;
    }


    /**
     * 初始化路径设置
     *
     * @param multiFolder
     */
    private void initFolder(String multiFolder) {
        blockHeadFolder = multiFolder + "/blockhead";
        blockTxFolder = multiFolder + "/blocktx";
        attrIndexFolder = multiFolder + "/attr_index";
        inBlockIndexFolder = multiFolder + "/inblock_index";
        attrIndexBackupFolder = multiFolder + "/index_backup";
        FileUtils.createFolder(blockHeadFolder + "/device");
        FileUtils.createFolder(blockHeadFolder + "/instruction");
        FileUtils.createFolder(blockHeadFolder + "/collect");
        FileUtils.createFolder(blockTxFolder + "/device");
        FileUtils.createFolder(blockTxFolder + "/instruction");
        FileUtils.createFolder(blockTxFolder + "/collect");
        FileUtils.createFolder(attrIndexFolder);
        FileUtils.createFolder(inBlockIndexFolder);
        FileUtils.createFolder(attrIndexBackupFolder);

        deviceIdIndexPath = attrIndexFolder + "/device_id.index";
        instIdIndexPath = attrIndexFolder + "/inst_id.index";
        collectIdIndexPath = attrIndexFolder + "/collect_id.index";
        instDidIndexPath = attrIndexFolder + "/inst_did.index";
        deviceDidIndexBackupPattern = attrIndexBackupFolder + "/block%d_device_id.index";
        instIdIndexBackupPattern = attrIndexBackupFolder + "/block%d_inst_id.index";
        collectIdIndexBackupPattern = attrIndexBackupFolder + "/block%d_collect_id.index";
        instDidIndexBackupPattern = attrIndexBackupFolder + "/block%d_inst_did.index";
    }

    public void closeIndexes() {
        deviceIdIndex.close();
        instDidIndex.close();
        instIdIndex.close();
        collectIdIndex.close();
    }


}
