package cc.qxt063.blockStorageSimulator.entity.semantics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class CollectedData {
    public Long cid;
    public Long did;
    public String cdata;
    public String csign;
}
