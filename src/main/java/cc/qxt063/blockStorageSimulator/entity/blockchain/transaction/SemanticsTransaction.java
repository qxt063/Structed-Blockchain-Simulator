package cc.qxt063.blockStorageSimulator.entity.blockchain.transaction;

import cc.qxt063.blockStorageSimulator.entity.blockchain.Attribute;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
public class SemanticsTransaction extends Transaction {
    public String structName;
    public ArrayList<Attribute> attributes;
    public Long versionNo;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }

}
