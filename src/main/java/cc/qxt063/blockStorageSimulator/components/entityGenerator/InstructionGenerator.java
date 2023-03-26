package cc.qxt063.blockStorageSimulator.components.entityGenerator;


import cc.qxt063.blockStorageSimulator.entity.semantics.Instruction;
import cc.qxt063.blockStorageSimulator.utils.RandomUtils;
import cc.qxt063.blockStorageSimulator.utils.SnowflakeIdUtils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class InstructionGenerator {
    private static final InstructionGenerator instance = new InstructionGenerator();
    private static final List<Long> workers = LongStream.rangeClosed(1, 500).boxed().collect(Collectors.toList());
    private DeviceGenerator dg;

    public InstructionGenerator() {
        dg = DeviceGenerator.getInstance();
    }

    public static InstructionGenerator getInstance() {
        return instance;
    }

    public Instruction getRandomOne() {
        Instruction ins = new Instruction();
        Long did = dg.getRandomAdded();
        if (did == null) return null;
        ins.setDid(did)
                .setIid(SnowflakeIdUtils.nextId(workers.get(RandomUtils.randomInt(workers.size()))))
                .setIdata(RandomUtils.randomString(50))
                .setIfrom(RandomUtils.randomString(20));
        return ins;
    }

}
