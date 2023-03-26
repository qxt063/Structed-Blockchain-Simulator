package cc.qxt063.blockStorageSimulator.utils;

/**
 * 64 位的 Snowflake ID
 */
public class SnowflakeIdUtils {
    private static final long EPOCH = 1647532800000L; // 设置开始时间戳为 2022-03-18 00:00:00
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    private static long sequence = 0L;
    private static long lastTimestamp = -1L;

    private static long workerId;
    private static long datacenterId = 9541255463L;

//    static {
//        workerId = Long.parseLong(System.getProperty("workerId", "0"));
//        datacenterId = Long.parseLong(System.getProperty("datacenterId", "0"));
//
//        if (workerId > MAX_WORKER_ID || workerId < 0) {
//            throw new IllegalArgumentException("workerId 必须在 [0, " + MAX_WORKER_ID + "] 范围内");
//        }
//
//        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
//            throw new IllegalArgumentException("datacenterId 必须在 [0, " + MAX_DATACENTER_ID + "] 范围内");
//        }
//    }

    public static synchronized long nextId(long workerId) {
        long timestamp = timeGen();

        if (timestamp < lastTimestamp) {
            throw new RuntimeException("时钟回拨，无法生成 ID");
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0L) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private static long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private static long timeGen() {
        return System.currentTimeMillis();
    }

}