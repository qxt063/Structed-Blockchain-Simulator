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
public class BlockMeta {
    public Long height;
    /**
     * genesis semantics data version
     */
    public String type;
    public Long timestamp;
    public String preHash;
    public String bodyHash;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
