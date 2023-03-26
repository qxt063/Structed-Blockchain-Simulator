package cc.qxt063.blockStorageSimulator.entity.blockchain.block.header;

import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.TreeMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
public class DataSpecial extends BlockSpecial {
    public String semanticsName;
    public String inBlockHash;
    public String pkHash;
    public TreeMap<String, String> buckets;
    public TreeMap<String, String> indexHash;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
