package cc.qxt063.blockStorageSimulator.entity.blockchain.transaction;

import cc.qxt063.blockStorageSimulator.utils.HashUtils;
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
public class DataTransaction extends Transaction {
    private String op;
    private TreeMap<String, String> attributeData;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

    public String getSha256() {
        return HashUtils.sha256Hex(this);
    }
}
