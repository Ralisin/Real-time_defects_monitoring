package ralisin.it.maps;

import org.apache.flink.api.common.functions.MapFunction;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import ralisin.it.model.PrintTile;

import java.util.HashMap;
import java.util.Map;

public class MsgpackBytesToRowMapper implements MapFunction<byte[], PrintTile> {

    @Override
    public PrintTile map(byte[] value) throws Exception {
        try {
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(value);

            Map<String, Object> fields = new HashMap<>();
            unpacker.unpackMapHeader();

            for (int i = 0; i < 5; i++) {
                String key = unpacker.unpackString();
                switch (key) {
                    case "print_id":
                        fields.put(key, unpacker.unpackString());
                        break;
                    case "batch_id":
                    case "tile_id":
                    case "layer":
                        fields.put(key, unpacker.unpackInt());
                        break;
                    case "tif":
                        int len = unpacker.unpackBinaryHeader();
                        byte[] tif = unpacker.readPayload(len);
                        fields.put("tif", tif);
                        break;
                }
            }
            unpacker.close();

            return new PrintTile(
                    (String) fields.get("print_id"),
                    (int) fields.get("batch_id"),
                    (int) fields.get("tile_id"),
                    (int) fields.get("layer"),
                    (byte[]) fields.get("tif")
            );
        } catch (Exception e) {
            System.err.println("[MsgpackBytesToRowMapper] Error: " + e);
            return new PrintTile(null, -1, -1, -1, null);
        }
    }
}
