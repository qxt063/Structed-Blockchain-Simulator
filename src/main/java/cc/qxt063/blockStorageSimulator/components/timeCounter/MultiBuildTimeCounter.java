package cc.qxt063.blockStorageSimulator.components.timeCounter;

import cc.qxt063.blockStorageSimulator.utils.CSVUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiBuildTimeCounter {
    /*
        出块时间分析:
        对于每一个区块：
        1. 属性索引插入时间
        2. 属性索引merkle计算时间(pages次)
        3. 块内索引插入时间
        4. 块内索引merkle计算时间(pages次)
        5. 交易merkle计算时间
     */
    private final String filePath;

    List<String> header = Arrays.asList("attr_insert", "attr_merkle", "in_block_insert", "in_block_merkle", "tx_merkle","io_time", "all_time");

    List<Long> attrInsertCount = new ArrayList<>();
    List<Long> attrMerkleCount = new ArrayList<>();
    List<Long> inBlockInsertCount = new ArrayList<>();
    List<Long> inBlockMerkleCount = new ArrayList<>();
    List<Long> txMerkleCount = new ArrayList<>();
    List<Long> ioCount = new ArrayList<>();
    /**
     * 计算所有时间包括区块io
     */
    List<Long> allTimeCount = new ArrayList<>();


//    Long attrInsertOnce = 0L;
//    Long attrMerkleOnce = 0L;
//    Long inBlockInsertOnce = 0L;
//    Long inBlockMerkleOnce = 0L;
//    Long txMerkleOnce = 0L;
//    Long allTimeOnce = 0L;


    public MultiBuildTimeCounter(String filePath) {
        this.filePath = filePath;
    }

    public void count(Long attrInsert, Long attrMerkle, Long inBlockInsert, Long inBlockMerkle, Long txMerkle, Long ioTime, Long allTime) {
        attrInsertCount.add(attrInsert);
        attrMerkleCount.add(attrMerkle);
        inBlockInsertCount.add(inBlockInsert);
        inBlockMerkleCount.add(inBlockMerkle);
        txMerkleCount.add(txMerkle);
        ioCount.add(ioTime);
        allTimeCount.add(allTime);
    }


//    public boolean flushOnce() {
//        if (attrInsertOnce == 0 || attrMerkleOnce == 0 || inBlockInsertOnce == 0 ||
//                inBlockMerkleOnce == 0 || txMerkleOnce == 0 || allTimeOnce == 0) {
//            System.err.println("Multi Timer未写入完整，刷新失败");
//            return false;
//        }
//        attrInsertCount.add(attrInsertOnce);
//        attrMerkleCount.add(attrMerkleOnce);
//        inBlockInsertCount.add(inBlockInsertOnce);
//        inBlockMerkleCount.add(inBlockMerkleOnce);
//        txMerkleCount.add(txMerkleOnce);
//        allTimeCount.add(allTimeOnce);
//        attrInsertOnce = 0L;
//        attrMerkleOnce = 0L;
//        inBlockInsertOnce = 0L;
//        inBlockMerkleOnce = 0L;
//        txMerkleOnce = 0L;
//        allTimeOnce = 0L;
//        return true;
//    }


    public void saveCsv() {
        List<List<Long>> tmp = new ArrayList<>();
        tmp.add(attrInsertCount);
        tmp.add(attrMerkleCount);
        tmp.add(inBlockInsertCount);
        tmp.add(inBlockMerkleCount);
        tmp.add(txMerkleCount);
        tmp.add(ioCount);
        tmp.add(allTimeCount);
        CSVUtils.lists2Csv(filePath, header, tmp);
    }


//    public void setAttrInsertOnce(Long attrInsertOnce) {
//        this.attrInsertOnce = attrInsertOnce;
//    }
//
//    public void setAttrMerkleOnce(Long attrMerkleOnce) {
//        this.attrMerkleOnce = attrMerkleOnce;
//    }
//
//    public void setInBlockInsertOnce(Long inBlockInsertOnce) {
//        this.inBlockInsertOnce = inBlockInsertOnce;
//    }
//
//    public void setInBlockMerkleOnce(Long inBlockMerkleOnce) {
//        this.inBlockMerkleOnce = inBlockMerkleOnce;
//    }
//
//    public void setTxMerkleOnce(Long txMerkleOnce) {
//        this.txMerkleOnce = txMerkleOnce;
//    }
//
//    public void setAllTimeOnce(Long allTimeOnce) {
//        this.allTimeOnce = allTimeOnce;
//    }
}
