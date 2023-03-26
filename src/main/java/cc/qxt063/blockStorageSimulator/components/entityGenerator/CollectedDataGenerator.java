package cc.qxt063.blockStorageSimulator.components.entityGenerator;

import cc.qxt063.blockStorageSimulator.entity.semantics.CollectedData;
import cc.qxt063.blockStorageSimulator.utils.RandomUtils;
import cc.qxt063.blockStorageSimulator.utils.SnowflakeIdUtils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class CollectedDataGenerator {
    private static final CollectedDataGenerator instance = new CollectedDataGenerator();
    private static final List<Long> workers = LongStream.rangeClosed(501, 1000).boxed().collect(Collectors.toList());
    private DeviceGenerator dg;

    public CollectedDataGenerator() {
        dg = DeviceGenerator.getInstance();
    }

    public static CollectedDataGenerator getInstance() {
        return instance;
    }

    public CollectedData getRandomOne() {
        CollectedData cd = new CollectedData();
        Long did = dg.getRandomAdded();
        if (did == null) return null;
        cd.setDid(did)
                .setCid(SnowflakeIdUtils.nextId(workers.get(RandomUtils.randomInt(workers.size()))))
                .setCdata(RandomUtils.randomString(50))
                .setCsign(RandomUtils.randomString(50));
        return cd;
    }

}
