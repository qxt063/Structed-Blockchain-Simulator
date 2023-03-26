package cc.qxt063.blockStorageSimulator.entity.blockchain.transaction;

import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class NullTransaction extends Transaction {
    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
