package cc.qxt063.blockStorageSimulator.components.timeCounter;

import cc.qxt063.blockStorageSimulator.utils.CSVUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SingleBuildTimeCounter {
    private final String filePath;

    List<String> header = Arrays.asList("merkle", "all_time");
    List<Long> merkleCount = new ArrayList<>();
    /**
     * 计算所有时间包括区块io
     */
    List<Long> allTimeCount = new ArrayList<>();

//    private Long merkleTimeOnce = 0L;
//    private Long allTimeOnce = 0L;


    public SingleBuildTimeCounter(String filePath) {
        this.filePath = filePath;
    }

    public void count(Long merkleTime, Long allTime) {
        merkleCount.add(merkleTime);
        allTimeCount.add(allTime);
    }

//    public boolean flushOnce() {
//        if (merkleTimeOnce == 0 || allTimeOnce == 0) {
//            System.err.println("Single Timer未写入完整，刷新失败");
//            return false;
//        }
//        merkleCount.add(merkleTimeOnce);
//        allTimeCount.add(allTimeOnce);
//        merkleTimeOnce = 0L;
//        allTimeOnce = 0L;
//        return true;
//    }

    public void saveCsv() {
        List<List<Long>> tmp = new ArrayList<>();
        tmp.add(merkleCount);
        tmp.add(allTimeCount);
        CSVUtils.lists2Csv(filePath, header, tmp);
    }

//    public void setMerkleTimeOnce(Long merkleTimeOnce) {
//        this.merkleTimeOnce = merkleTimeOnce;
//    }
//
//    public void setAllTimeOnce(Long allTimeOnce) {
//        this.allTimeOnce = allTimeOnce;
//    }
}
