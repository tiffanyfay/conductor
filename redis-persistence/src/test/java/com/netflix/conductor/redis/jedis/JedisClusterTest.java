/*
 * Copyright 2020 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.redis.jedis;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JedisClusterTest {

    private final redis.clients.jedis.JedisCluster mockCluster =
            mock(redis.clients.jedis.JedisCluster.class);
    private final JedisCluster jedisCluster = new JedisCluster(mockCluster);

    @Test
    void set() {
        jedisCluster.set("key", "value");
        jedisCluster.set("key", "value", SetParams.setParams());
    }

    @Test
    void get() {
        jedisCluster.get("key");
    }

    @Test
    void exists() {
        jedisCluster.exists("key");
    }

    @Test
    void persist() {
        jedisCluster.persist("key");
    }

    @Test
    void type() {
        jedisCluster.type("key");
    }

    @Test
    void expire() {
        jedisCluster.expire("key", 1337);
    }

    @Test
    void pexpire() {
        jedisCluster.pexpire("key", 1337);
    }

    @Test
    void expireAt() {
        jedisCluster.expireAt("key", 1337);
    }

    @Test
    void pexpireAt() {
        jedisCluster.pexpireAt("key", 1337);
    }

    @Test
    void ttl() {
        jedisCluster.ttl("key");
    }

    @Test
    void pttl() {
        jedisCluster.pttl("key");
    }

    @Test
    void setbit() {
        jedisCluster.setbit("key", 1337, "value");
        jedisCluster.setbit("key", 1337, true);
    }

    @Test
    void getbit() {
        jedisCluster.getbit("key", 1337);
    }

    @Test
    void setrange() {
        jedisCluster.setrange("key", 1337, "value");
    }

    @Test
    void getrange() {
        jedisCluster.getrange("key", 1337, 1338);
    }

    @Test
    void getSet() {
        jedisCluster.getSet("key", "value");
    }

    @Test
    void setnx() {
        jedisCluster.setnx("test", "value");
    }

    @Test
    void setex() {
        jedisCluster.setex("key", 1337, "value");
    }

    @Test
    void psetex() {
        jedisCluster.psetex("key", 1337, "value");
    }

    @Test
    void decrBy() {
        jedisCluster.decrBy("key", 1337);
    }

    @Test
    void decr() {
        jedisCluster.decr("key");
    }

    @Test
    void incrBy() {
        jedisCluster.incrBy("key", 1337);
    }

    @Test
    void incrByFloat() {
        jedisCluster.incrByFloat("key", 1337);
    }

    @Test
    void incr() {
        jedisCluster.incr("key");
    }

    @Test
    void append() {
        jedisCluster.append("key", "value");
    }

    @Test
    void substr() {
        jedisCluster.substr("key", 1337, 1338);
    }

    @Test
    void hset() {
        jedisCluster.hset("key", "field", "value");
    }

    @Test
    void hget() {
        jedisCluster.hget("key", "field");
    }

    @Test
    void hsetnx() {
        jedisCluster.hsetnx("key", "field", "value");
    }

    @Test
    void hmset() {
        jedisCluster.hmset("key", new HashMap<>());
    }

    @Test
    void hmget() {
        jedisCluster.hmget("key", "fields");
    }

    @Test
    void hincrBy() {
        jedisCluster.hincrBy("key", "field", 1337);
    }

    @Test
    void hincrByFloat() {
        jedisCluster.hincrByFloat("key", "field", 1337);
    }

    @Test
    void hexists() {
        jedisCluster.hexists("key", "field");
    }

    @Test
    void hdel() {
        jedisCluster.hdel("key", "field");
    }

    @Test
    void hlen() {
        jedisCluster.hlen("key");
    }

    @Test
    void hkeys() {
        jedisCluster.hkeys("key");
    }

    @Test
    void hvals() {
        jedisCluster.hvals("key");
    }

    @Test
    void ggetAll() {
        jedisCluster.hgetAll("key");
    }

    @Test
    void rpush() {
        jedisCluster.rpush("key", "string");
    }

    @Test
    void lpush() {
        jedisCluster.lpush("key", "string");
    }

    @Test
    void llen() {
        jedisCluster.llen("key");
    }

    @Test
    void lrange() {
        jedisCluster.lrange("key", 1337, 1338);
    }

    @Test
    void ltrim() {
        jedisCluster.ltrim("key", 1337, 1338);
    }

    @Test
    void lindex() {
        jedisCluster.lindex("key", 1337);
    }

    @Test
    void lset() {
        jedisCluster.lset("key", 1337, "value");
    }

    @Test
    void lrem() {
        jedisCluster.lrem("key", 1337, "value");
    }

    @Test
    void lpop() {
        jedisCluster.lpop("key");
    }

    @Test
    void rpop() {
        jedisCluster.rpop("key");
    }

    @Test
    void sadd() {
        jedisCluster.sadd("key", "member");
    }

    @Test
    void smembers() {
        jedisCluster.smembers("key");
    }

    @Test
    void srem() {
        jedisCluster.srem("key", "member");
    }

    @Test
    void spop() {
        jedisCluster.spop("key");
        jedisCluster.spop("key", 1337);
    }

    @Test
    void scard() {
        jedisCluster.scard("key");
    }

    @Test
    void sismember() {
        jedisCluster.sismember("key", "member");
    }

    @Test
    void srandmember() {
        jedisCluster.srandmember("key");
        jedisCluster.srandmember("key", 1337);
    }

    @Test
    void strlen() {
        jedisCluster.strlen("key");
    }

    @Test
    void zadd() {
        jedisCluster.zadd("key", new HashMap<>());
        jedisCluster.zadd("key", new HashMap<>(), ZAddParams.zAddParams());
        jedisCluster.zadd("key", 1337, "members");
        jedisCluster.zadd("key", 1337, "members", ZAddParams.zAddParams());
    }

    @Test
    void zrange() {
        jedisCluster.zrange("key", 1337, 1338);
    }

    @Test
    void zrem() {
        jedisCluster.zrem("key", "member");
    }

    @Test
    void zincrby() {
        jedisCluster.zincrby("key", 1337, "member");
        jedisCluster.zincrby("key", 1337, "member", ZIncrByParams.zIncrByParams());
    }

    @Test
    void zrank() {
        jedisCluster.zrank("key", "member");
    }

    @Test
    void zrevrank() {
        jedisCluster.zrevrank("key", "member");
    }

    @Test
    void zrevrange() {
        jedisCluster.zrevrange("key", 1337, 1338);
    }

    @Test
    void zrangeWithScores() {
        jedisCluster.zrangeWithScores("key", 1337, 1338);
    }

    @Test
    void zrevrangeWithScores() {
        jedisCluster.zrevrangeWithScores("key", 1337, 1338);
    }

    @Test
    void zcard() {
        jedisCluster.zcard("key");
    }

    @Test
    void zscore() {
        jedisCluster.zscore("key", "member");
    }

    @Test
    void sort() {
        jedisCluster.sort("key");
        jedisCluster.sort("key", new SortingParams());
    }

    @Test
    void zcount() {
        jedisCluster.zcount("key", "min", "max");
        jedisCluster.zcount("key", 1337, 1338);
    }

    @Test
    void zrangeByScore() {
        jedisCluster.zrangeByScore("key", "min", "max");
        jedisCluster.zrangeByScore("key", 1337, 1338);
        jedisCluster.zrangeByScore("key", "min", "max", 1337, 1338);
        jedisCluster.zrangeByScore("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrevrangeByScore() {
        jedisCluster.zrevrangeByScore("key", "max", "min");
        jedisCluster.zrevrangeByScore("key", 1337, 1338);
        jedisCluster.zrevrangeByScore("key", "max", "min", 1337, 1338);
        jedisCluster.zrevrangeByScore("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrangeByScoreWithScores() {
        jedisCluster.zrangeByScoreWithScores("key", "min", "max");
        jedisCluster.zrangeByScoreWithScores("key", "min", "max", 1337, 1338);
        jedisCluster.zrangeByScoreWithScores("key", 1337, 1338);
        jedisCluster.zrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrevrangeByScoreWithScores() {
        jedisCluster.zrevrangeByScoreWithScores("key", "max", "min");
        jedisCluster.zrevrangeByScoreWithScores("key", "max", "min", 1337, 1338);
        jedisCluster.zrevrangeByScoreWithScores("key", 1337, 1338);
        jedisCluster.zrevrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zremrangeByRank() {
        jedisCluster.zremrangeByRank("key", 1337, 1338);
    }

    @Test
    void zremrangeByScore() {
        jedisCluster.zremrangeByScore("key", "start", "end");
        jedisCluster.zremrangeByScore("key", 1337, 1338);
    }

    @Test
    void zlexcount() {
        jedisCluster.zlexcount("key", "min", "max");
    }

    @Test
    void zrangeByLex() {
        jedisCluster.zrangeByLex("key", "min", "max");
        jedisCluster.zrangeByLex("key", "min", "max", 1337, 1338);
    }

    @Test
    void zrevrangeByLex() {
        jedisCluster.zrevrangeByLex("key", "max", "min");
        jedisCluster.zrevrangeByLex("key", "max", "min", 1337, 1338);
    }

    @Test
    void zremrangeByLex() {
        jedisCluster.zremrangeByLex("key", "min", "max");
    }

    @Test
    void linsert() {
        jedisCluster.linsert("key", ListPosition.AFTER, "pivot", "value");
    }

    @Test
    void lpushx() {
        jedisCluster.lpushx("key", "string");
    }

    @Test
    void rpushx() {
        jedisCluster.rpushx("key", "string");
    }

    @Test
    void blpop() {
        jedisCluster.blpop(1337, "arg");
    }

    @Test
    void brpop() {
        jedisCluster.brpop(1337, "arg");
    }

    @Test
    void del() {
        jedisCluster.del("key");
    }

    @Test
    void echo() {
        jedisCluster.echo("string");
    }

    @Test
    void move() {
        assertThrows(UnsupportedOperationException.class, () -> {
            jedisCluster.move("key", 1337);
        });
    }

    @Test
    void bitcount() {
        jedisCluster.bitcount("key");
        jedisCluster.bitcount("key", 1337, 1338);
    }

    @Test
    void bitpos() {
        assertThrows(UnsupportedOperationException.class, () -> {
            jedisCluster.bitpos("key", true);
        });
    }

    @Test
    void hscan() {
        jedisCluster.hscan("key", "cursor");

        ScanResult<Entry<byte[], byte[]>> scanResult =
                new ScanResult<>(
                        "cursor".getBytes(),
                        Arrays.asList(
                                new AbstractMap.SimpleEntry<>("key1".getBytes(), "val1".getBytes()),
                                new AbstractMap.SimpleEntry<>(
                                        "key2".getBytes(), "val2".getBytes())));

        when(mockCluster.hscan(Mockito.any(), Mockito.any(), Mockito.any(ScanParams.class)))
                .thenReturn(scanResult);
        ScanResult<Map.Entry<String, String>> result =
                jedisCluster.hscan("key", "cursor", new ScanParams());

        assertEquals("cursor", result.getCursor());
        assertEquals(2, result.getResult().size());
        assertEquals("val1", result.getResult().get(0).getValue());
    }

    @Test
    void sscan() {
        jedisCluster.sscan("key", "cursor");

        ScanResult<byte[]> scanResult =
                new ScanResult<>(
                        "sscursor".getBytes(), Arrays.asList("val1".getBytes(), "val2".getBytes()));

        when(mockCluster.sscan(Mockito.any(), Mockito.any(), Mockito.any(ScanParams.class)))
                .thenReturn(scanResult);

        ScanResult<String> result = jedisCluster.sscan("key", "cursor", new ScanParams());
        assertEquals("sscursor", result.getCursor());
        assertEquals(2, result.getResult().size());
        assertEquals("val1", result.getResult().get(0));
    }

    @Test
    void zscan() {
        jedisCluster.zscan("key", "cursor");
        jedisCluster.zscan("key", "cursor", new ScanParams());
    }

    @Test
    void pfadd() {
        jedisCluster.pfadd("key", "elements");
    }

    @Test
    void pfcount() {
        jedisCluster.pfcount("key");
    }

    @Test
    void geoadd() {
        jedisCluster.geoadd("key", new HashMap<>());
        jedisCluster.geoadd("key", 1337, 1338, "member");
    }

    @Test
    void geodist() {
        jedisCluster.geodist("key", "member1", "member2");
        jedisCluster.geodist("key", "member1", "member2", GeoUnit.KM);
    }

    @Test
    void geohash() {
        jedisCluster.geohash("key", "members");
    }

    @Test
    void geopos() {
        jedisCluster.geopos("key", "members");
    }

    @Test
    void georadius() {
        jedisCluster.georadius("key", 1337, 1338, 32, GeoUnit.KM);
        jedisCluster.georadius("key", 1337, 1338, 32, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    void georadiusByMember() {
        jedisCluster.georadiusByMember("key", "member", 1337, GeoUnit.KM);
        jedisCluster.georadiusByMember(
                "key", "member", 1337, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    void bitfield() {
        jedisCluster.bitfield("key", "arguments");
    }
}
