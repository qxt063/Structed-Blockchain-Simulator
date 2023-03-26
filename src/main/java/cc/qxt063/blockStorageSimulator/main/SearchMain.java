package cc.qxt063.blockStorageSimulator.main;

import cc.qxt063.blockStorageSimulator.components.search.MultiSearcher;
import cc.qxt063.blockStorageSimulator.components.search.Searcher;
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

    private static final List<String> single_header = Arrays.asList("value_index", "value_no_index", "range_index",
            "range_no_index", "connect_double_index", "connect_single_index", "connect_no_index");
    private static final List<String> multi_header = Arrays.asList("value_index", "value_no_index", "range_index",
            "range_no_index", "connect_double_index", "connect_single_index", "connect_no_index",
            "range_index_noIO", "range_no_index_noIO");

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
                        multi_connect_double_index = new ArrayList<>(),
                        multi_connect_single_index = new ArrayList<>(),
                        multi_connect_no_index = new ArrayList<>(),

                        multi_range_index_noIO = new ArrayList<>(),
                        multi_range_no_index_noIO = new ArrayList<>();

                for (int blockMax : blockMaxConf) {
                    Searcher ss = new SingleSearcher(blockMax, Folder.SINGLE);
                    MultiSearcher ms = new MultiSearcher(blockMax, Folder.MULTI);

                    System.out.println("================== " + i + "." + selected + "-" + blockMax + " ==================");
                    // 等值
                    long did = RandomUtils.randomInt(1, 10001);
                    System.out.printf("value: %d\n\n", did);
                    System.out.println("a) index value");
//                    single_value_index.add(ss.searchInstByDid(did));
//                    multi_value_index.add(ms.searchInstByDid(did));
                    single_value_index.add(0L);
                    multi_value_index.add(ms.searchInstByDid(did));

                    System.out.println("b) no index value");
//                    single_value_no_index.add(ss.searchCollectByDid(did));
//                    multi_value_no_index.add(ms.searchCollectByDid(did));
                    single_value_no_index.add(0L);
                    multi_value_no_index.add(ms.searchCollectByDid(did));
                    // 范围
                    long min = RandomUtils.randomInt(1, 9000), max = RandomUtils.randomInt((int) (min + 1), 10001);
                    System.out.printf("\nrange: [%d,%d]\n\n", min, max);

                    System.out.println("c) index range");
//                    single_range_index.add(ss.searchRangeInstByDid(min, max));
//                    multi_range_index.add(ms.searchRangeInstByDid(min, max));
                    single_range_index.add(0L);
                    multi_range_index.add(ms.searchRangeInstByDid(min, max));
                    multi_range_index_noIO.add(ms.searchRangeInstByDid_noIO(min, max));

                    System.out.println("d) no index range");
//                    single_range_no_index.add(ss.searchRangeCollectByDid(min, max));
//                    multi_range_no_index.add(ms.searchRangeCollectByDid(min, max));
                    single_range_no_index.add(0L);
                    multi_range_no_index.add(ms.searchRangeCollectByDid(min, max));
                    multi_range_no_index_noIO.add(ms.searchRangeCollectByDid_noIO(min, max));

                    //连接
//                    System.out.println("e) double index connect");
//                    single_connect_double_index.add(ss.searchDeviceDidEqualsInstDid());
//                    multi_connect_double_index.add(ms.searchDeviceDidEqualsInstDid());
                    single_connect_double_index.add(0L);
                    multi_connect_double_index.add(0L);

//                    System.out.println("f) single index connect");
//                    single_connect_single_index.add(ss.searchDeviceDidEqualsCollectDid());
//                    multi_connect_single_index.add(ms.searchDeviceDidEqualsCollectDid());
                    single_connect_single_index.add(0L);
                    multi_connect_single_index.add(0L);

//                    System.out.println("g) no index connect");
//                    single_connect_no_index.add(ss.searchCollectDidEqualsCollectDid());
//                    multi_connect_no_index.add(ms.searchCollectDidEqualsCollectDid());
                    single_connect_no_index.add(0L);
                    multi_connect_no_index.add(0L);

                    System.out.println();

//                    System.out.println("save to csv\n");
//                    List<List<Long>> single = Arrays.asList(single_value_index, single_value_no_index, single_range_index, single_range_no_index,
//                            single_connect_double_index, single_connect_single_index, single_connect_no_index);
//                    List<List<Long>> multi = Arrays.asList(multi_value_index, multi_value_no_index, multi_range_index, multi_range_no_index,
//                            multi_connect_double_index, multi_connect_single_index, multi_connect_no_index);
//                    // single
//                    CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "single_" + i + "_step_" + blockMax + ".csv",
//                            header, single);
//                    // multi
//                    CSVUtils.lists2Csv(Folder.RESULTS + "/" + selected + "/" + "multi_" + i + "_step_" + blockMax + ".csv",
//                            header, multi);
                }
                System.out.println("save to csv");
                List<List<Long>> single = Arrays.asList(single_value_index, single_value_no_index, single_range_index, single_range_no_index,
                        single_connect_double_index, single_connect_single_index, single_connect_no_index);
                List<List<Long>> multi = Arrays.asList(multi_value_index, multi_value_no_index, multi_range_index, multi_range_no_index,
                        multi_connect_double_index, multi_connect_single_index, multi_connect_no_index,
                        multi_range_index_noIO, multi_range_no_index_noIO);
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
