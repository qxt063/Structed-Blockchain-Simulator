package cc.qxt063.blockStorageSimulator.main;

import cc.qxt063.blockStorageSimulator.config.Folder;

public class GeneratorMain {
    private static int txInBlockNum = 4000;
    private static int blockNum = 1000;
    private static String folder = "tx4000";

    public static void main(String[] args) {
        Folder.changeFolder(folder);
        // 生成raw区块并写入文件夹
//        new RawTxGenerator(Folder.RAW, txInBlockNum).genMultiBlock(blockNum);
//        // 生成single区块
//        new Raw2SingleConverter(Folder.SINGLE).genSingleBlockFromRaw(Folder.RAW);
//        // 生成multi区块
//        Raw2MultiConverter multiConverter = new Raw2MultiConverter(Folder.MULTI,50);
//        multiConverter.genMultiBlocksFromRaw(Folder.RAW);
//        multiConverter.closeIndexes();
    }
}
