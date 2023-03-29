package cc.qxt063.blockStorageSimulator.main;

import cc.qxt063.blockStorageSimulator.components.search.MultiSearcher;
import cc.qxt063.blockStorageSimulator.components.search.SingleSearcher;
import cc.qxt063.blockStorageSimulator.config.Folder;
import cc.qxt063.blockStorageSimulator.utils.CSVUtils;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.RandomUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SearchMain {
    private static final String[] selectedTxNums = {"tx500", "tx1000", "tx2000", "tx4000"}; //folder
    private static final int[] blockMaxConf = {50, 100, 150, 200, 250, 300, 350, 400, 450, 500,
            550, 600, 650, 700, 750, 800, 850, 900, 950, 1000};
    private static final long searchRange = 2000; // 范围搜索长度

    private static final List<String> single_header = Arrays.asList("value_index", "value_no_index", "range_index",
            "range_no_index", "connect_double_index", "connect_single_index", "connect_no_index");
    private static final List<String> multi_header = Arrays.asList("value_index", "value_no_index",
            String.format("range%d_index", searchRange), String.format("range%d_no_index", searchRange),
            String.format("range%d_index_IO", searchRange), String.format("range%d_no_index_IO", searchRange),
            "connect_double_index", "connect_single_index", "connect_no_index",
            "connect_double_IO", "connect_single_IO", "connect_single_bucket", "connect_no_index_IO");

    private static final List<String> blockHeightCSV = Arrays.stream(blockMaxConf).mapToObj(String::valueOf).collect(Collectors.toList());

    public static void main(String[] args) {
//        Folder.changeFolder(folder);

//        Searcher ss = new SingleSearcher(blockMax, Folder.SINGLE),
//                ms = new MultiSearcher(blockMax, Folder.MULTI);
//        List<Searcher> searchers = Arrays.asList(ss, ms);

        for (int i = 0; i < 1; i++) {

            for (String selected : selectedTxNums) {
                Folder.changeFolder(selected);
                FileUtils.createFolder(Folder.RESULTS + "/" + selected);

                List<Long> single_value_index = new ArrayList<>(), single_value_no_index = new ArrayList<>(),
                        single_range_index = new ArrayList<>(), single_range_no_index = new ArrayList<>(),
                        single_connect_double_index = new ArrayList<>(),
                        single_connect_single_index = new ArrayList<>(),
                        single_connect_no_index = new ArrayList<>(),

                        multi_value_index = new ArrayList<>(), multi_value_no_index = new ArrayList<>(),
                        multi_range_index = new ArrayList<>(), multi_range_no_index = new ArrayList<>(),

                        multi_range_index_IO = new ArrayList<>(),
                        multi_range_no_index_IO = new ArrayList<>(),

                        multi_connect_double_index = new ArrayList<>(),
                        multi_connect_single_index = new ArrayList<>(),
                        multi_connect_no_index = new ArrayList<>(),

                        connect_double_IO = new ArrayList<>(),
                        connect_single_IO = new ArrayList<>(),
                        connect_single_bucket = new ArrayList<>(),
                        connect_no_index_IO = new ArrayList<>();

                for (int blockMax : blockMaxConf) {
                    SingleSearcher ss = new SingleSearcher(blockMax, Folder.SINGLE);
                    MultiSearcher ms = new MultiSearcher(blockMax, Folder.MULTI);

                    System.out.println("================== " + i + "." + selected + "-" + blockMax + " ==================");
                    // ================= 等值 =================
                    long did = RandomUtils.randomInt(1, 10001);
                    System.out.printf("value: %d\n", did);
                    System.out.println("a) index value");
//                    single_value_index.add(ss.searchInstByDid(did));
//                    multi_value_index.add(ms.searchInstByDid(did));
                    single_value_index.add(0L);
                    multi_value_index.add(0L);

                    System.out.println("b) no index value");
//                    single_value_no_index.add(ss.searchCollectByDid(did));
//                    multi_value_no_index.add(ms.searchCollectByDid(did));
                    single_value_no_index.add(0L);
                    multi_value_no_index.add(0L);

                    // ================= 范围 =================
//                    long min = RandomUtils.randomInt(1, 9000), max = RandomUtils.randomInt((int) (min + 1), 10001);
                    long min = 1000, max = min + searchRange; // 2000,4000,6000,8000
                    System.out.printf("\nrange: [%d,%d]\n", min, max);

                    System.out.println("c) index range");
//                    single_range_index.add(ss.searchRangeInstByDid(min, max));
                    long[] indexRangeTime = ms.searchRangeInstByDid_withIOTime(min, max);
//                    long[] indexRangeTime = ms.searchRangeInstByDid_noMerge_withIOTime(min, max);
                    multi_range_index.add(indexRangeTime[0]);
                    multi_range_index_IO.add(indexRangeTime[1]);

                    single_range_index.add(0L);
//                    multi_range_index.add(0L);
//                    multi_range_index_IO.add(0L);

                    System.out.println("d) no index range");
//                    single_range_no_index.add(ss.searchRangeCollectByDid(min, max));
                    long[] noIndexRangeTime = ms.searchRangeCollectByDid_withIOTime(min, max);
                    multi_range_no_index.add(noIndexRangeTime[0]);
                    multi_range_no_index_IO.add(noIndexRangeTime[1]);
                    single_range_no_index.add(0L);
//                    multi_range_no_index.add(0L);
//                    multi_range_no_index_IO.add(0L);

                    // ================= 连接 =================
                    System.out.println("e) double index connect");
//                    single_connect_double_index.add(ss.searchDeviceDidEqualsInstDid());
//                    long[] connect_double_time = ms.searchDeviceDidEqualsInstDid_withIOTime();
//                    multi_connect_double_index.add(connect_double_time[0]);
//                    connect_double_IO.add(connect_double_time[1]);
                    single_connect_double_index.add(0L);
                    multi_connect_double_index.add(0L);
                    connect_double_IO.add(0L);

                    System.out.println("f) single index connect");
//                    single_connect_single_index.add(ss.searchDeviceDidEqualsCollectDid());
//                    long[] single_time = ms.searchDeviceDidEqualsCollectDid_withIOTime();
//                    multi_connect_single_index.add(single_time[0]);
//                    connect_single_IO.add(single_time[1]);
//                    connect_single_bucket.add(single_time[2]);
                    single_connect_single_index.add(0L);
                    multi_connect_single_index.add(0L);
                    connect_single_IO.add(0L);
                    connect_single_bucket.add(0L);

                    System.out.println("g) no index connect");
//                    single_connect_no_index.add(ss.searchCollectDidEqualsCollectDid());
//                    long[] no_time = ms.searchCollectDidEqualsCollectDid_withIOTime();
//                    multi_connect_no_index.add(no_time[0]);
//                    connect_no_IO.add(no_time[1]);
                    single_connect_no_index.add(0L);
                    multi_connect_no_index.add(0L);
                    connect_no_index_IO.add(0L);

                    System.out.println();

//                    System.out.println("save to csv\n");
//                    List<List<Long>> single = Arrays.asList(single_value_index, single_value_no_index, single_range_index, single_range_no_index,
//                            single_connect_double_index, single_connect_single_index, single_connect_no_index);
//                    List<List<Long>> multi = Arrays.asList(multi_value_index, multi_value_no_index, multi_range_index, multi_range_no_index,
//                            multi_connect_double_index, multi_connect_single_index, multi_connect_no_index,
//                            multi_range_index_noIO, multi_range_no_index_noIO,
//                            connect_double_IO, connect_single_IO, connect_single_bucket);
//                    // single
//                    CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "single_" + i + "_step_" + blockMax + ".csv",
//                            single_header, single);
//                    // multi
//                    CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "multi_" + i + "_step_" + blockMax + ".csv",
//                            multi_header, multi);
                }
                System.out.println("save to csv");
                List<List<Long>> single = Arrays.asList(single_value_index, single_value_no_index, single_range_index, single_range_no_index,
                        single_connect_double_index, single_connect_single_index, single_connect_no_index);
                List<List<Long>> multi = Arrays.asList(multi_value_index, multi_value_no_index,
                        multi_range_index, multi_range_no_index,
                        multi_range_index_IO, multi_range_no_index_IO,
                        multi_connect_double_index, multi_connect_single_index, multi_connect_no_index,
                        connect_double_IO, connect_single_IO, connect_single_bucket, connect_no_index_IO);
                // single
                CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "single_" + i + ".csv",
                        single_header, single);
                // multi
                CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "multi_" + i + ".csv",
                        multi_header, multi);
            }

        }

    }
}
