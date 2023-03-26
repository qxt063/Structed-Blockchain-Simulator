package cc.qxt063.blockStorageSimulator.components.blockGenerator;

import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.semantics.CollectedData;
import cc.qxt063.blockStorageSimulator.entity.semantics.Device;
import cc.qxt063.blockStorageSimulator.entity.semantics.Instruction;
import cc.qxt063.blockStorageSimulator.components.entityGenerator.CollectedDataGenerator;
import cc.qxt063.blockStorageSimulator.components.entityGenerator.DeviceGenerator;
import cc.qxt063.blockStorageSimulator.components.entityGenerator.InstructionGenerator;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.ObjectUtils;
import cc.qxt063.blockStorageSimulator.utils.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/*
区块数，区块内交易数
原始区块以文件夹 块高 表示，原始交易存放于区块文件夹内
 */

/**
 * 生成原始交易，以文件夹形式组织
 */
public class RawTxGenerator {
    private String rawFolder;
    private Integer txInBlockNum, deviceNum;
    private CollectedDataGenerator collectGen;
    private DeviceGenerator deviceGen;
    private InstructionGenerator instGen;

    private RawTxGenerator() {
        collectGen = CollectedDataGenerator.getInstance();
        deviceGen = DeviceGenerator.getInstance();
        instGen = InstructionGenerator.getInstance();
    }

    public RawTxGenerator(String rawFolder, int txInBlockNum) {
        this();
        this.rawFolder = rawFolder;
        this.txInBlockNum = txInBlockNum;
        deviceNum = txInBlockNum / 5;
    }

    public void genMultiBlock(int blockNum) {
        for (int i = 0; i < blockNum; i++) {
            String blockFolder = rawFolder + "/" + i;
            FileUtils.createFolder(blockFolder);
            genBlock(blockFolder);
        }
    }

    private void genBlock(String blockFolder) {
        int remain = txInBlockNum;
        List<DataTransaction> devList = new ArrayList<>(deviceNum),
                insList = new ArrayList<>(txInBlockNum),
                collectList = new ArrayList<>(txInBlockNum);

        // 生成device
        if (deviceGen.getRemain() != 0) {
            for (int i = 0; i < deviceNum; i++) {
                Device d = deviceGen.getRandomOne();
                if (d == null) break;
                DataTransaction dt = new DataTransaction("insert", getStringMap(d));
                dt.setTimestamp(System.currentTimeMillis());
                devList.add(dt);
                remain--;
            }
        }

        // 计算其余两种数量
        int instNum = RandomUtils.randomInt(remain);
        int collectNum = remain - instNum;

        // 生成instruction
        for (int i = 0; i < instNum; i++) {
            Instruction ins = instGen.getRandomOne();
            DataTransaction it = new DataTransaction("insert", getStringMap(ins));
            it.setTimestamp(System.currentTimeMillis());
            insList.add(it);
        }
        // 生成collectData
        for (int i = 0; i < collectNum; i++) {
            CollectedData coll = collectGen.getRandomOne();
            DataTransaction ct = new DataTransaction("insert", getStringMap(coll));
            ct.setTimestamp(System.currentTimeMillis());
            collectList.add(ct);
        }

        writToFile(devList, insList, collectList, blockFolder);
    }

    private void writToFile(List<DataTransaction> devList, List<DataTransaction> insList, List<DataTransaction> collectList,
                            String blockFolder) {
        FileUtils.createFolder(blockFolder + "/device");
        FileUtils.createFolder(blockFolder + "/instruction");
        FileUtils.createFolder(blockFolder + "/collect");


        for (int i = 0; i < devList.size(); i++) {
            FileUtils.writeToFile(blockFolder + "/device/" + i, devList.get(i).toString());
        }
        for (int i = 0; i < insList.size(); i++) {
            FileUtils.writeToFile(blockFolder + "/instruction/" + i, insList.get(i).toString());
        }
        for (int i = 0; i < collectList.size(); i++) {
            FileUtils.writeToFile(blockFolder + "/collect/" + i, collectList.get(i).toString());
        }
    }


    private <T> TreeMap<String, String> getStringMap(T t) {
        try {
            return ObjectUtils.toStringMap(t);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
