package cc.qxt063.blockStorageSimulator.config;

public class Folder {
    private static final String ROOT = "/home/sayumi/blockdata/";
//    private static final String ROOT = "/mnt/ramdisk/";

//    private static String selected = "/test";

    public static String RAW;
    public static String SINGLE,MULTI, RESULTS;
//    /**
//     * 区块文件夹
//     */
//    public static String MULTI_BLOCK;
//    /**
//     * 以文件形式模拟随机读取交易的区块文件夹
//     */
//    public static String MULTI_RANDOM_TX;
//    /**
//     * 属性索引
//     * 包含pk,attr等
//     */
//    public static String MULTI_ATT_INDEX;
//    /**
//     * 块内索引
//     */
//    public static String MULTI_INBLOCK_INDEX;

    public static void changeFolder(String folder) {
        String SELECTED = ROOT + folder;
        RAW = SELECTED + "/raw";
        SINGLE = SELECTED + "/single";
        MULTI = SELECTED + "/multi";
        RESULTS = ROOT + "/results";
//        MULTI_BLOCK = SELECTED + "/multi/block";
//        MULTI_ATT_INDEX = SELECTED + "/multi/att_index";
//        MULTI_INBLOCK_INDEX = SELECTED + "/multi/inblock_index";
    }

//    public static void changeFolder(String folder) {
//        refresh(folder);
//    }
}
