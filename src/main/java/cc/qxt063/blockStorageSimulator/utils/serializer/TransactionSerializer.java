package cc.qxt063.blockStorageSimulator.utils.serializer;

import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.DataTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.NullTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.SemanticsTransaction;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import com.google.gson.*;

import java.lang.reflect.Type;

public class TransactionSerializer implements JsonDeserializer<Transaction>, JsonSerializer<Transaction> {
    @Override
    public Transaction deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (jsonElement.getAsJsonObject().has("op")) { //数据交易
            return jsonDeserializationContext.deserialize(jsonElement, DataTransaction.class);
        } else if (jsonElement.getAsJsonObject().has("structName")) { //语义交易
            return jsonDeserializationContext.deserialize(jsonElement, SemanticsTransaction.class);
        }
        return jsonDeserializationContext.deserialize(jsonElement, NullTransaction.class);
    }

    @Override
    public JsonElement serialize(Transaction transaction, Type type, JsonSerializationContext jsonSerializationContext) {
        if (transaction instanceof DataTransaction)
            return JsonUtils.toJsonElement((DataTransaction) transaction);
        else if (transaction instanceof SemanticsTransaction)
            return JsonUtils.toJsonElement((SemanticsTransaction) transaction);
        else if (transaction instanceof NullTransaction)
            return JsonUtils.toJsonElement((NullTransaction) transaction);
        return null;
    }
}
