package cc.qxt063.blockStorageSimulator.components.bpt.bptree;

/**
 * Wrapper to conveniently return the (Key, Value) pair
 * without having to resort to "weird" solutions.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class KVPair {

    private final long k;           // key
    private final String v;       // value

    /**
     * This is the only constructor... as we only
     * need to set them
     *
     * @param k   the key of (K, V) pair
     * @param v the value of the (K, V) pair
     */
    public KVPair(long k, String v) {
        this.k = k;
        this.v = v;
    }

    public long getK() {
        return k;
    }

    public String getV() {
        return v;
    }

    @Override
    public String toString() {
        return "KV(" + k + ", " + v + ')';
    }
}
