package cc.qxt063.blockStorageSimulator.entity.blockchain.block.header;

import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class BlockHeader {
    public BlockMeta meta;
    public BlockSpecial special;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
