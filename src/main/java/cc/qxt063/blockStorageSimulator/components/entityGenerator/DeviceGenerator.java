package cc.qxt063.blockStorageSimulator.components.entityGenerator;

import cc.qxt063.blockStorageSimulator.entity.semantics.Device;
import cc.qxt063.blockStorageSimulator.utils.RandomUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class DeviceGenerator {
    private static final DeviceGenerator instance = new DeviceGenerator();
    private static final LinkedList<Long> dids = LongStream.rangeClosed(1, 10000).boxed().collect(Collectors.toCollection(LinkedList::new));
    private static final List<Long> didAdded = new ArrayList<>(10001);


    public DeviceGenerator() {
    }

    public static DeviceGenerator getInstance() {
        return instance;
    }

    public Device getRandomOne() {
        if (dids.size() == 0) return null;
        int i = RandomUtils.randomInt(dids.size());
        Device d = new Device();
        d.setDid(dids.remove(i))
                .setDname(RandomUtils.randomString(10))
                .setDdesp(RandomUtils.randomString(20))
                .setDkey(RandomUtils.randomString(50));
        didAdded.add(d.did);
        return d;
    }

    public Long getRandomAdded() {
        if (didAdded.size() == 0) return null;
        return didAdded.get(RandomUtils.randomInt(didAdded.size()));
    }

    public int getRemain() {
        return dids.size();
    }

}
