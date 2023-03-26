package cc.qxt063.blockStorageSimulator.entity.blockchain;

import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * 语义交易-语义属性
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Attribute {
    public String name;
    public String type;
    public Boolean pk;
    public Boolean unique;
    public Boolean index;
    public Long rangeMin;
    public Long rangeMax;
    public Integer bucket;

    @Override
    public String toString() {
        return JsonUtils.toJson(this);
    }
}
