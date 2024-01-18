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

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JedisSentinelTest {

    private final Jedis jedis = mock(Jedis.class);
    private final JedisSentinelPool jedisPool = mock(JedisSentinelPool.class);
    private final JedisSentinel jedisSentinel = new JedisSentinel(jedisPool);

    @BeforeEach
    void init() {
        when(this.jedisPool.getResource()).thenReturn(this.jedis);
    }

    @Test
    void set() {
        jedisSentinel.set("key", "value");
        jedisSentinel.set("key", "value", SetParams.setParams());
    }

    @Test
    void get() {
        jedisSentinel.get("key");
    }

    @Test
    void exists() {
        jedisSentinel.exists("key");
    }

    @Test
    void persist() {
        jedisSentinel.persist("key");
    }

    @Test
    void type() {
        jedisSentinel.type("key");
    }

    @Test
    void expire() {
        jedisSentinel.expire("key", 1337);
    }

    @Test
    void pexpire() {
        jedisSentinel.pexpire("key", 1337);
    }

    @Test
    void expireAt() {
        jedisSentinel.expireAt("key", 1337);
    }

    @Test
    void pexpireAt() {
        jedisSentinel.pexpireAt("key", 1337);
    }

    @Test
    void ttl() {
        jedisSentinel.ttl("key");
    }

    @Test
    void pttl() {
        jedisSentinel.pttl("key");
    }

    @Test
    void setbit() {
        jedisSentinel.setbit("key", 1337, "value");
        jedisSentinel.setbit("key", 1337, true);
    }

    @Test
    void getbit() {
        jedisSentinel.getbit("key", 1337);
    }

    @Test
    void setrange() {
        jedisSentinel.setrange("key", 1337, "value");
    }

    @Test
    void getrange() {
        jedisSentinel.getrange("key", 1337, 1338);
    }

    @Test
    void getSet() {
        jedisSentinel.getSet("key", "value");
    }

    @Test
    void setnx() {
        jedisSentinel.setnx("test", "value");
    }

    @Test
    void setex() {
        jedisSentinel.setex("key", 1337, "value");
    }

    @Test
    void psetex() {
        jedisSentinel.psetex("key", 1337, "value");
    }

    @Test
    void decrBy() {
        jedisSentinel.decrBy("key", 1337);
    }

    @Test
    void decr() {
        jedisSentinel.decr("key");
    }

    @Test
    void incrBy() {
        jedisSentinel.incrBy("key", 1337);
    }

    @Test
    void incrByFloat() {
        jedisSentinel.incrByFloat("key", 1337);
    }

    @Test
    void incr() {
        jedisSentinel.incr("key");
    }

    @Test
    void append() {
        jedisSentinel.append("key", "value");
    }

    @Test
    void substr() {
        jedisSentinel.substr("key", 1337, 1338);
    }

    @Test
    void hset() {
        jedisSentinel.hset("key", "field", "value");
    }

    @Test
    void hget() {
        jedisSentinel.hget("key", "field");
    }

    @Test
    void hsetnx() {
        jedisSentinel.hsetnx("key", "field", "value");
    }

    @Test
    void hmset() {
        jedisSentinel.hmset("key", new HashMap<>());
    }

    @Test
    void hmget() {
        jedisSentinel.hmget("key", "fields");
    }

    @Test
    void hincrBy() {
        jedisSentinel.hincrBy("key", "field", 1337);
    }

    @Test
    void hincrByFloat() {
        jedisSentinel.hincrByFloat("key", "field", 1337);
    }

    @Test
    void hexists() {
        jedisSentinel.hexists("key", "field");
    }

    @Test
    void hdel() {
        jedisSentinel.hdel("key", "field");
    }

    @Test
    void hlen() {
        jedisSentinel.hlen("key");
    }

    @Test
    void hkeys() {
        jedisSentinel.hkeys("key");
    }

    @Test
    void hvals() {
        jedisSentinel.hvals("key");
    }

    @Test
    void ggetAll() {
        jedisSentinel.hgetAll("key");
    }

    @Test
    void rpush() {
        jedisSentinel.rpush("key", "string");
    }

    @Test
    void lpush() {
        jedisSentinel.lpush("key", "string");
    }

    @Test
    void llen() {
        jedisSentinel.llen("key");
    }

    @Test
    void lrange() {
        jedisSentinel.lrange("key", 1337, 1338);
    }

    @Test
    void ltrim() {
        jedisSentinel.ltrim("key", 1337, 1338);
    }

    @Test
    void lindex() {
        jedisSentinel.lindex("key", 1337);
    }

    @Test
    void lset() {
        jedisSentinel.lset("key", 1337, "value");
    }

    @Test
    void lrem() {
        jedisSentinel.lrem("key", 1337, "value");
    }

    @Test
    void lpop() {
        jedisSentinel.lpop("key");
    }

    @Test
    void rpop() {
        jedisSentinel.rpop("key");
    }

    @Test
    void sadd() {
        jedisSentinel.sadd("key", "member");
    }

    @Test
    void smembers() {
        jedisSentinel.smembers("key");
    }

    @Test
    void srem() {
        jedisSentinel.srem("key", "member");
    }

    @Test
    void spop() {
        jedisSentinel.spop("key");
        jedisSentinel.spop("key", 1337);
    }

    @Test
    void scard() {
        jedisSentinel.scard("key");
    }

    @Test
    void sismember() {
        jedisSentinel.sismember("key", "member");
    }

    @Test
    void srandmember() {
        jedisSentinel.srandmember("key");
        jedisSentinel.srandmember("key", 1337);
    }

    @Test
    void strlen() {
        jedisSentinel.strlen("key");
    }

    @Test
    void zadd() {
        jedisSentinel.zadd("key", new HashMap<>());
        jedisSentinel.zadd("key", new HashMap<>(), ZAddParams.zAddParams());
        jedisSentinel.zadd("key", 1337, "members");
        jedisSentinel.zadd("key", 1337, "members", ZAddParams.zAddParams());
    }

    @Test
    void zrange() {
        jedisSentinel.zrange("key", 1337, 1338);
    }

    @Test
    void zrem() {
        jedisSentinel.zrem("key", "member");
    }

    @Test
    void zincrby() {
        jedisSentinel.zincrby("key", 1337, "member");
        jedisSentinel.zincrby("key", 1337, "member", ZIncrByParams.zIncrByParams());
    }

    @Test
    void zrank() {
        jedisSentinel.zrank("key", "member");
    }

    @Test
    void zrevrank() {
        jedisSentinel.zrevrank("key", "member");
    }

    @Test
    void zrevrange() {
        jedisSentinel.zrevrange("key", 1337, 1338);
    }

    @Test
    void zrangeWithScores() {
        jedisSentinel.zrangeWithScores("key", 1337, 1338);
    }

    @Test
    void zrevrangeWithScores() {
        jedisSentinel.zrevrangeWithScores("key", 1337, 1338);
    }

    @Test
    void zcard() {
        jedisSentinel.zcard("key");
    }

    @Test
    void zscore() {
        jedisSentinel.zscore("key", "member");
    }

    @Test
    void sort() {
        jedisSentinel.sort("key");
        jedisSentinel.sort("key", new SortingParams());
    }

    @Test
    void zcount() {
        jedisSentinel.zcount("key", "min", "max");
        jedisSentinel.zcount("key", 1337, 1338);
    }

    @Test
    void zrangeByScore() {
        jedisSentinel.zrangeByScore("key", "min", "max");
        jedisSentinel.zrangeByScore("key", 1337, 1338);
        jedisSentinel.zrangeByScore("key", "min", "max", 1337, 1338);
        jedisSentinel.zrangeByScore("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrevrangeByScore() {
        jedisSentinel.zrevrangeByScore("key", "max", "min");
        jedisSentinel.zrevrangeByScore("key", 1337, 1338);
        jedisSentinel.zrevrangeByScore("key", "max", "min", 1337, 1338);
        jedisSentinel.zrevrangeByScore("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrangeByScoreWithScores() {
        jedisSentinel.zrangeByScoreWithScores("key", "min", "max");
        jedisSentinel.zrangeByScoreWithScores("key", "min", "max", 1337, 1338);
        jedisSentinel.zrangeByScoreWithScores("key", 1337, 1338);
        jedisSentinel.zrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zrevrangeByScoreWithScores() {
        jedisSentinel.zrevrangeByScoreWithScores("key", "max", "min");
        jedisSentinel.zrevrangeByScoreWithScores("key", "max", "min", 1337, 1338);
        jedisSentinel.zrevrangeByScoreWithScores("key", 1337, 1338);
        jedisSentinel.zrevrangeByScoreWithScores("key", 1337, 1338, 1339, 1340);
    }

    @Test
    void zremrangeByRank() {
        jedisSentinel.zremrangeByRank("key", 1337, 1338);
    }

    @Test
    void zremrangeByScore() {
        jedisSentinel.zremrangeByScore("key", "start", "end");
        jedisSentinel.zremrangeByScore("key", 1337, 1338);
    }

    @Test
    void zlexcount() {
        jedisSentinel.zlexcount("key", "min", "max");
    }

    @Test
    void zrangeByLex() {
        jedisSentinel.zrangeByLex("key", "min", "max");
        jedisSentinel.zrangeByLex("key", "min", "max", 1337, 1338);
    }

    @Test
    void zrevrangeByLex() {
        jedisSentinel.zrevrangeByLex("key", "max", "min");
        jedisSentinel.zrevrangeByLex("key", "max", "min", 1337, 1338);
    }

    @Test
    void zremrangeByLex() {
        jedisSentinel.zremrangeByLex("key", "min", "max");
    }

    @Test
    void linsert() {
        jedisSentinel.linsert("key", ListPosition.AFTER, "pivot", "value");
    }

    @Test
    void lpushx() {
        jedisSentinel.lpushx("key", "string");
    }

    @Test
    void rpushx() {
        jedisSentinel.rpushx("key", "string");
    }

    @Test
    void blpop() {
        jedisSentinel.blpop(1337, "arg");
    }

    @Test
    void brpop() {
        jedisSentinel.brpop(1337, "arg");
    }

    @Test
    void del() {
        jedisSentinel.del("key");
    }

    @Test
    void echo() {
        jedisSentinel.echo("string");
    }

    @Test
    void move() {
        jedisSentinel.move("key", 1337);
    }

    @Test
    void bitcount() {
        jedisSentinel.bitcount("key");
        jedisSentinel.bitcount("key", 1337, 1338);
    }

    @Test
    void bitpos() {
        jedisSentinel.bitpos("key", true);
    }

    @Test
    void hscan() {
        jedisSentinel.hscan("key", "cursor");
        jedisSentinel.hscan("key", "cursor", new ScanParams());
    }

    @Test
    void sscan() {
        jedisSentinel.sscan("key", "cursor");
        jedisSentinel.sscan("key", "cursor", new ScanParams());
    }

    @Test
    void zscan() {
        jedisSentinel.zscan("key", "cursor");
        jedisSentinel.zscan("key", "cursor", new ScanParams());
    }

    @Test
    void pfadd() {
        jedisSentinel.pfadd("key", "elements");
    }

    @Test
    void pfcount() {
        jedisSentinel.pfcount("key");
    }

    @Test
    void geoadd() {
        jedisSentinel.geoadd("key", new HashMap<>());
        jedisSentinel.geoadd("key", 1337, 1338, "member");
    }

    @Test
    void geodist() {
        jedisSentinel.geodist("key", "member1", "member2");
        jedisSentinel.geodist("key", "member1", "member2", GeoUnit.KM);
    }

    @Test
    void geohash() {
        jedisSentinel.geohash("key", "members");
    }

    @Test
    void geopos() {
        jedisSentinel.geopos("key", "members");
    }

    @Test
    void georadius() {
        jedisSentinel.georadius("key", 1337, 1338, 32, GeoUnit.KM);
        jedisSentinel.georadius("key", 1337, 1338, 32, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    void georadiusByMember() {
        jedisSentinel.georadiusByMember("key", "member", 1337, GeoUnit.KM);
        jedisSentinel.georadiusByMember(
                "key", "member", 1337, GeoUnit.KM, GeoRadiusParam.geoRadiusParam());
    }

    @Test
    void bitfield() {
        jedisSentinel.bitfield("key", "arguments");
    }
}
