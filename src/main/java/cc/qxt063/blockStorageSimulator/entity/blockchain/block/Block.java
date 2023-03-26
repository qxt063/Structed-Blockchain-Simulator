package cc.qxt063.blockStorageSimulator.entity.blockchain.block;

import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockHeader;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.HashUtils;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Block {
    public BlockHeader header;
    /**
     * body简化了合约信息，直接用交易数组
     */
    public ArrayList<Transaction> body;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

    public String getSha256() {
        return HashUtils.sha256Hex(this);
    }

}
