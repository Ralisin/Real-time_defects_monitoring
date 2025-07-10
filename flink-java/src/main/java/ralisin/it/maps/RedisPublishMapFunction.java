package ralisin.it.maps;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

public class RedisPublishMapFunction<T> extends RichMapFunction<T, T> {
    private final String channelName;
    private final String redisHost;
    private final int redisPort;
    private final int redisDb;

    private transient Jedis jedis;

    public RedisPublishMapFunction(String channelName) {
        this(channelName, "redis", 6379, 0);
    }

    public RedisPublishMapFunction(String channelName, String redisHost, int redisPort, int redisDb) {
        this.channelName = channelName;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDb = redisDb;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.jedis = new Jedis(redisHost, redisPort);
        this.jedis.select(redisDb);
        try {
            this.jedis.ping();
            System.out.printf("[RedisPublishMapFunction] Redis connection established for channel: %s%n", channelName);
        } catch (Exception e) {
            System.err.printf("[RedisPublishMapFunction] Error connecting to Redis: %s%n", e.getMessage());
            throw e;
        }
    }

    @Override
    public T map(T value) {
        try {
            if (jedis == null) throw new RuntimeException("Redis client not initialized");
            // Puoi personalizzare la serializzazione qui
            String msg = value.toString();
            Long subs = jedis.publish(channelName, msg);
            // (Se vuoi, logga)
            // System.out.printf("[RedisPublishMapFunction] Published to %s: %s (subs: %d)%n", channelName, msg.length() > 100 ? msg.substring(0, 100) : msg, subs);
            return value;
        } catch (Exception e) {
            System.err.printf("[RedisPublishMapFunction] Error publishing to Redis channel %s: %s%n", channelName, e.getMessage());
            // Tentativo di reconnessione
            try {
                this.jedis = new Jedis(redisHost, redisPort);
                this.jedis.select(redisDb);
                this.jedis.publish(channelName, value.toString());
            } catch (Exception reconnectError) {
                System.err.printf("[RedisPublishMapFunction] Failed to reconnect and publish: %s%n", reconnectError.getMessage());
            }
            return value;
        }
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
            System.out.printf("[RedisPublishMapFunction] Redis connection closed for channel: %s%n", channelName);
        }
    }
}
