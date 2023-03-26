import cc.qxt063.blockStorageSimulator.components.bpt.TreeIndex;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusConfiguration;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusTree;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusTreePerformanceCounter;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.KVPair;
import cc.qxt063.blockStorageSimulator.components.bpt.util.InvalidBTreeStateException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

public class TreeIndexTest {
    @Test
    public void createAndBackupTest() {
        String folder = "/home/sayumi/blockdata/test/index_test";
        TreeIndex index = new TreeIndex(folder + "/test.index", true, true);
        for (long i = 1L; i <= 50; i++) {
            index.insert(100 + i, Long.toString(i));
            if (i % 10 == 0) {
                index.backupIndex(folder + "/backup_" + i / 10 + ".index");
            }
        }
        index.close();
        for (int i = 1; i <= 5; i++) {
            TreeIndex itest = new TreeIndex(folder + "/backup_" + i + ".index", true, false);
            System.out.println("=========================");
            List<String> keys0 = itest.findByKey(100 + i * 10L - 100);
            System.out.println(keys0);
            List<String> keys1 = itest.findByKey(100 + i * 10L - 5);
            System.out.println(keys1);
            List<String> keys2 = itest.findByKey(100 + i * 10L + 5);
            System.out.println(keys2);
            TreeMap<Long, List<String>> res0 = itest.findByRange(i * 10L - 5, i * 10L - 1);
            System.out.println(res0);
            TreeMap<Long, List<String>> res1 = itest.findByRange(100 + i * 10L - 5, 100 + i * 10L - 1);
            System.out.println(res1);
            TreeMap<Long, List<String>> res2 = itest.findByRange(100 + i * 10L + 1, 100 + i * 10L + 5);
            System.out.println(res2);
            itest.close();
        }
    }

    @Test
    public void travelTest() throws InvalidBTreeStateException, IOException {
        String filePath = "/home/sayumi/blockdata/tx500/multi/attr_index/inst_did.index";
        boolean recreateTree = false;
        BPlusConfiguration bConf = new BPlusConfiguration(1024);
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(false);
        BPlusTree tree = new BPlusTree(bConf, recreateTree ? "rw+" : "rw", filePath, bPerf);
//        tree.printLeafNodes();
        for (List<KVPair> kvs : tree) {
            System.out.println(kvs);
//            System.out.printf("%d:%s\n", kvw.getKey(), kvw.getValue());
        }
        for (List<KVPair> kvs : tree) {
            System.out.println(kvs);
//            System.out.printf("%d:%s\n", kvw.getKey(), kvw.getValue());
        }
    }
}
