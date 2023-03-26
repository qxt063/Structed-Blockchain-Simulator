package cc.qxt063.blockStorageSimulator.utils;

import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.transaction.Transaction;
import cc.qxt063.blockStorageSimulator.utils.serializer.BlockSpecialSerializer;
import cc.qxt063.blockStorageSimulator.utils.serializer.TransactionSerializer;
import com.google.gson.*;

public class JsonUtils {
    static GsonBuilder gb = new GsonBuilder();
    static Gson gson;

    static {
        // 启用属性的pretty printing和字段命名策略
//        gsonBuilder.setPrettyPrinting();
        gb.setFieldNamingPolicy(FieldNamingPolicy.IDENTITY);
        gb.registerTypeAdapter(BlockSpecial.class, new BlockSpecialSerializer());
        gb.registerTypeAdapter(Transaction.class, new TransactionSerializer());
        gson = gb.create();
    }

    public static <T> String toJson(T t) {
//        Gson gson = gb.create();zz
        return gson.toJson(t);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }

    public static <T> JsonElement toJsonElement(T t) {
        return gson.toJsonTree(t);
    }
}
