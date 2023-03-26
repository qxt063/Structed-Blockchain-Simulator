import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockHeader;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockMeta;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.DataSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.HashUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import cc.qxt063.blockStorageSimulator.utils.MerkleUtils;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

public class EntityTest {

    @Test
    public void hash() {
        System.out.println(HashUtils.sha256Hex("0"));
    }

    @Test
    public void checkHash() {
        ArrayList<Transaction> dts = new ArrayList<>();
        dts.add(new DataTransaction("insert", new TreeMap<>(Map.of("sno", "123", "sname", "alice"))));


        String merkleRoot = MerkleUtils.buildMerkleHex(dts);

        BlockHeader header = new BlockHeader(
                new BlockMeta(1L, "data", System.currentTimeMillis(), "0000000000000000000000", merkleRoot),
                new DataSpecial("student", "inBlockHash", "pkHash",
                        new TreeMap<>(Map.of("sno", "1000000000", "sname", "1000000000")),
                        new TreeMap<>(Map.of("sname", "snameIndexHash")))
        );
        Block block = new Block(header, dts);
        String jsonstr = JsonUtils.toJson(block);
        System.out.println(jsonstr);
        Block serialBlock = JsonUtils.fromJson(jsonstr, Block.class);
        System.out.println(serialBlock);
    }

    @Test
    public void readFromFileTest() {
        String dJson = FileUtils.readFromFile("/home/sayumi/blockdata/test/raw/0/device/0");
        String iJson = FileUtils.readFromFile("/home/sayumi/blockdata/test/raw/0/instruction/0");
        String cJson = FileUtils.readFromFile("/home/sayumi/blockdata/test/raw/0/collect/0");

        Transaction dt = JsonUtils.fromJson(dJson, Transaction.class);
        Transaction it = JsonUtils.fromJson(iJson, Transaction.class);
        Transaction ct = JsonUtils.fromJson(cJson, Transaction.class);

        System.out.println(dJson);
        System.out.println(dt);
        System.out.println("===================");
        System.out.println(iJson);
        System.out.println(it);
        System.out.println("===================");
        System.out.println(cJson);
        System.out.println(ct);

    }

    @Test
    public void groupingByTes() {
        List<Integer> l = Arrays.asList(1, 10, 20, 30, 50, 70, 90);
        Map<Integer, List<Integer>> buckedTrans = l.stream().collect(Collectors.groupingBy(n -> n / 10));
        System.out.println(buckedTrans);
    }
}
