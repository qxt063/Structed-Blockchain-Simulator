import cc.qxt063.blockStorageSimulator.entity.blockchain.block.Block;
import cc.qxt063.blockStorageSimulator.utils.FileUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import org.junit.Test;

public class DeleteBody {
    //    private static final String blockFolder = "/home/sayumi/blockdata/tx500/multi/blockhead/collect";
    private static final String[] blockFolders = {
            "/home/sayumi/blockdata/tx500/multi/blockhead/device",
            "/home/sayumi/blockdata/tx500/multi/blockhead/collect",
            "/home/sayumi/blockdata/tx500/multi/blockhead/instruction",
            "/home/sayumi/blockdata/tx1000/multi/blockhead/device",
            "/home/sayumi/blockdata/tx1000/multi/blockhead/collect",
            "/home/sayumi/blockdata/tx1000/multi/blockhead/instruction",
            "/home/sayumi/blockdata/tx2000/multi/blockhead/device",
            "/home/sayumi/blockdata/tx2000/multi/blockhead/collect",
            "/home/sayumi/blockdata/tx2000/multi/blockhead/instruction",
            "/home/sayumi/blockdata/tx4000/multi/blockhead/device",
            "/home/sayumi/blockdata/tx4000/multi/blockhead/collect",
            "/home/sayumi/blockdata/tx4000/multi/blockhead/instruction",
    };

    @Test
    public void deleteBlockBody() {
        for (String folder : blockFolders) {
            for (String file : FileUtils.getAllFiles(folder)) {
                Block b = JsonUtils.fromJson(FileUtils.readFromFile(file), Block.class);
                b.setBody(null);
                FileUtils.writeToFile(file, JsonUtils.toJson(b));
                System.out.println(file + "  finished");
            }
        }
    }
}
