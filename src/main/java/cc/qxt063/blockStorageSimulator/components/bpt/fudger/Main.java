package cc.qxt063.blockStorageSimulator.components.bpt.fudger;

import cc.qxt063.blockStorageSimulator.components.bpt.util.InvalidBTreeStateException;
import cc.qxt063.blockStorageSimulator.components.bpt.util.TestRunner;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusConfiguration;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusTree;
import cc.qxt063.blockStorageSimulator.components.bpt.bptree.BPlusTreePerformanceCounter;

import java.io.IOException;

public class Main {

    public static void main(String[] args)
            throws IOException, InvalidBTreeStateException {
        boolean fastTrials = true;
        boolean recreateTree = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, recreateTree ? "rw+" : "rw", bPerf);

        //int tlen = 20000;
        //long skey = 0;
        //long eKey = tlen;
        //String val = "1234567890";
        //boolean unique = true;
        bt.printCurrentConfiguration();
//        if(recreateTree) {
//            Utilities.sequentialAddToTree(skey, eKey,
//                    val, unique, bt);
//            bPerf.printTotalStatistics();
//        }

        if(fastTrials)
            {
                TestRunner.runDefaultTrialsFast(bPerf);}
        else
            {TestRunner.runBench(bPerf);}

        System.out.println("\n -- Total pages in the end: " + bt.getTotalTreePages());
        // finally close it.
        bt.commitTree();

    }

}
