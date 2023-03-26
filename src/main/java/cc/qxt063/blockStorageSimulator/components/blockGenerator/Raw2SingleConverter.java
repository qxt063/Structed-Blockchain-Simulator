package cc.qxt063.blockStorageSimulator.components.blockGenerator;

import cc.qxt063.blockStorageSimulator.components.timeCounter.SingleBuildTimeCounter;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockHeader;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockMeta;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.HashUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import cc.qxt063.blockStorageSimulator.utils.MerkleUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 原始交易数据转为单链
 */
public class Raw2SingleConverter {
    private String singleBlockFolder;

    // 计时器
    private SingleBuildTimeCounter timer;
    private Long merkleTimeOnce = 0L;
    private Long allTimeOnce = 0L;

    public Raw2SingleConverter(String singleFolder) {
        this.singleBlockFolder = singleFolder + "/block";
        FileUtils.createFolder(singleBlockFolder);
        timer = new SingleBuildTimeCounter(singleFolder + "/time.csv");
    }

    /**
     * 转换Raw下的所有区块
     *
     * @param rawPath
     */
    public void genSingleBlockFromRaw(String rawPath) {
        String preHash = "0000000000000000000000000000000000000000000000000000000000000000";
        for (int i = 0; i < FileUtils.getFolderCount(rawPath); i++) {
            Block b = readBlockFromFolder((long) i + 1, preHash, "data", rawPath + "/" + i);
            preHash = HashUtils.sha256Hex(b);
            FileUtils.writeToFile(singleBlockFolder + '/' + (i + 1), b.toString());
            System.out.printf("%d: %s\n", i + 1, preHash);

            allTimeOnce = System.currentTimeMillis() - allTimeOnce;
            timer.count(merkleTimeOnce, allTimeOnce);
            merkleTimeOnce = allTimeOnce = 0L;
        }
        timer.saveCsv();
    }

    /**
     * 生成并写入一个区块
     *
     * @param height
     * @param preHash
     * @param type
     * @param rawFolder
     * @return
     */
    @SuppressWarnings("DuplicatedCode")
    private Block readBlockFromFolder(Long height, String preHash, String type, String rawFolder) {
        List<Transaction> devList = new ArrayList<>(), instList = new ArrayList<>(), colleList = new ArrayList<>();
        String dPath = rawFolder + "/device", iPath = rawFolder + "/instruction", cPath = rawFolder + "/collect";
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

        allTimeOnce = System.currentTimeMillis();
        return buildBlock(height, preHash, type, devList, instList, colleList);
    }

    private Block buildBlock(Long height, String preHash, String type,
                             List<Transaction> devList, List<Transaction> instList, List<Transaction> colleList) {
        Block b = new Block();
        ArrayList<Transaction> allTransactions = new ArrayList<>();
        allTransactions.addAll(devList);
        allTransactions.addAll(instList);
        allTransactions.addAll(colleList);
        allTransactions.sort((x, y) -> x.getTimestamp().compareTo(y.timestamp));

        // 计算merkle
        merkleTimeOnce = System.currentTimeMillis();
        String merkleRoot = MerkleUtils.buildMerkleHex(allTransactions);
        merkleTimeOnce = System.currentTimeMillis() - merkleTimeOnce;

        BlockHeader header = new BlockHeader();
        header.setMeta(new BlockMeta(height, type, System.currentTimeMillis(), preHash, merkleRoot));
//        header.setMeta(new BlockMeta(height, type, 1679168152390L, preHash, merkleRoot));
        BlockSpecial bs = null;
//        if ("data".equals(type)) {
//            bs = new DataSpecial();
//        }
        header.setSpecial(bs);
        b.setHeader(header);
        b.setBody(allTransactions);

        return b;
    }
}
