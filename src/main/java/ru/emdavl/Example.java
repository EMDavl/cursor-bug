package ru.emdavl;

import io.lettuce.core.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.*;

public class Example {
    public static void main(String[] args) {
        RedisClusterClient client = RedisClusterClient.create(List.of(
                RedisURI.create("127.0.0.1", 7000),
                RedisURI.create("127.0.0.1", 7001),
                RedisURI.create("127.0.0.1", 7002),
                RedisURI.create("127.0.0.1", 7003),
                RedisURI.create("127.0.0.1", 7004),
                RedisURI.create("127.0.0.1", 7005)
        ));
        StatefulRedisClusterConnection<String, String> anyConnection = client.connect();
        anyConnection.setReadFrom(ReadFrom.ANY);
        RedisAdvancedClusterCommands<String, String> sync = anyConnection.sync();

        final String zsetName = "my-zset4";

        for (int i = 0; i < 10000; i++) {
            sync.zadd(zsetName, i, "value" + i);
        }

        Set<String> set = new HashSet<>();
        ScoredValueScanCursor<String> cursor = null;
        do  {
            if (cursor == null) {
                cursor = sync.zscan(zsetName, ScanCursor.of("0"), ScanArgs.Builder.limit(1000));
            } else {
                cursor = sync.zscan(zsetName, ScanCursor.of(cursor.getCursor()), ScanArgs.Builder.limit(1000));
            }

            for (ScoredValue<String> value : cursor.getValues()) {
                set.add(value.getValue());
            }

        } while (!cursor.isFinished());

        System.out.println("Entries read: " + set.size());

        anyConnection.close();
        client.shutdown();
    }
}