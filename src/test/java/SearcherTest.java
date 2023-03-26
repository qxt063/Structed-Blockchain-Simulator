import cc.qxt063.blockStorageSimulator.components.search.MultiSearcher;
import cc.qxt063.blockStorageSimulator.components.search.Searcher;
import cc.qxt063.blockStorageSimulator.components.search.SingleSearcher;
import org.junit.Test;

public class SearcherTest {
    private static final String multiPath = "/home/sayumi/blockdata/tx500/multi";
    private static final String singlePath = "/home/sayumi/blockdata/tx500/single";
    private static final int blockMax = 500;

    private Searcher ss = new SingleSearcher(blockMax, singlePath),
            ms = new MultiSearcher(blockMax, multiPath);

    @Test
    public void singleValueTest() {
        System.out.println(ss.searchCollectByDid(365));
    }

    @Test
    public void singleConnectTest() {
        System.out.println(ss.searchDeviceDidEqualsInstDid());
    }

    @Test
    public void multiValueTest() {
        System.out.println(ms.searchInstByDid(365));
//        System.out.println(ms.searchCollectByDid(365));
    }

    @Test
    public void multiRangeTest() {
//        System.out.println(ms.searchRangeInstByDid(300, 400));
        System.out.println(ms.searchRangeCollectByDid(1300, 2500));
    }

    @Test
    public void multiConnectDoubleIndex() {
        System.out.println(ms.searchDeviceDidEqualsInstDid());
    }

    @Test
    public void multiConnectSingleIndex(){
        System.out.println(ms.searchDeviceDidEqualsCollectDid());
    }

    @Test
    public void multiConnectZeroIndex(){
        System.out.println(ms.searchCollectDidEqualsCollectDid());
    }


}
