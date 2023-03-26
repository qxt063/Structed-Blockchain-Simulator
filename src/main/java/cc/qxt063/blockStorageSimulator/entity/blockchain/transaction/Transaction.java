package cc.qxt063.blockStorageSimulator.entity.blockchain.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class Transaction {
    public Long timestamp;
}
