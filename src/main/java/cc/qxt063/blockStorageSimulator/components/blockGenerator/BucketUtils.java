package cc.qxt063.blockStorageSimulator.components.blockGenerator;

public class BucketUtils {
    /*
        did
        0 -> 1~1000
        1 -> 1001~2000
        ...
     */
    public static long getDidBucket(long did) {
        return (did - 1) / 1000;
    }

    public static long[] getDidBucketRange(long bucketNum) {
        return new long[]{bucketNum * 1000 + 1, (bucketNum + 1) * 1000};
    }

}
