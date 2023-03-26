package cc.qxt063.blockStorageSimulator.utils.serializer;

import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.BlockSpecial;
import cc.qxt063.blockStorageSimulator.entity.blockchain.block.header.DataSpecial;
import cc.qxt063.blockStorageSimulator.utils.JsonUtils;
import com.google.gson.*;

import java.lang.reflect.Type;

public class BlockSpecialSerializer implements JsonDeserializer<BlockSpecial>, JsonSerializer<BlockSpecial> {
    @Override
    public BlockSpecial deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (jsonElement.getAsJsonObject().has("semanticsName")) { //数据sp
            DataSpecial dsp = jsonDeserializationContext.deserialize(jsonElement, DataSpecial.class);
            return dsp;
        }
        return null;
    }

    @Override
    public JsonElement serialize(BlockSpecial blockSpecial, Type type, JsonSerializationContext jsonSerializationContext) {
        if (blockSpecial instanceof DataSpecial)
            return JsonUtils.toJsonElement((DataSpecial) blockSpecial);
        return null;
    }

//    @Override
//    public JsonElement serialize(BlockSpecial blockSpecial, Type type, JsonSerializationContext jsonSerializationContext) {
//        return null;
//    }
}
