package com.city.infra.redis;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

import java.util.*;

public class RedisAccessorTest extends TestCase {

    private RedisClusterFactory clusterFactory;

    private String prefix;

    @Before
    public void setUp() throws Exception {
        clusterFactory = new RedisClusterFactory();

        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7639));
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7640));

        Set<HostAndPort> standbyJedisClusterNodes = new HashSet<>();
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7543));
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7544));

        clusterFactory.setJedisClusterNodes(jedisClusterNodes);
        clusterFactory.setStandbyJedisClusterNodes(standbyJedisClusterNodes);

        prefix = String.format("accessor:%s:", UUID.randomUUID());
    }

    @Test
    public void testGet() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.set("test_get_key1", "value1", 3600);
        Assert.assertEquals("value1", redisAccessor.get("test_get_key1", "default_value"));
        Assert.assertEquals("default_value", redisAccessor.get("test_get_key2", "default_value"));
        Assert.assertEquals("", redisAccessor.get("test_get_key2", ""));
        Assert.assertNull(redisAccessor.get("test_get_key2", null));
    }

    @Test
    public void testSet() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.set("test_set_key1", "value1", 3600);
        redisAccessor.set("test_set_key2", "value2", 30);
        Assert.assertEquals("value1", redisAccessor.get("test_set_key1", "default_value"));
        Assert.assertEquals("value2", redisAccessor.get("test_set_key2", "default_value"));
        Thread.sleep(100 * 1000);
        Assert.assertEquals("default_value", redisAccessor.get("test_set_key2", "default_value"));
    }

    @Test
    public void testDelete() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.set("test_delete_key", "value", 3600);
        Assert.assertEquals("value", redisAccessor.get("test_delete_key", "default_value"));
        redisAccessor.delete("test_delete_key");
        Assert.assertEquals("default_value", redisAccessor.get("test_delete_key", "default_value"));
    }

    @Test
    public void testIncrby() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        Assert.assertEquals(5, redisAccessor.incrby("test_incrby_key", 5, 0, 3600));
        Assert.assertEquals(10, redisAccessor.incrby("test_incrby_key", 5, 0, 3600));
        Assert.assertEquals("10", redisAccessor.get("test_incrby_key", "0"));
    }

    @Test
    public void testHget() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.hset("test_hget_key1", "field1", "value1", 3600);
        Assert.assertEquals("value1", redisAccessor.hget("test_hget_key1", "field1", "default_value"));
        Assert.assertEquals("default_value", redisAccessor.hget("test_hget_key1", "field2", "default_value"));
        Assert.assertEquals("default_value", redisAccessor.hget("test_hget_key2", "field1", "default_value"));
        Assert.assertEquals("", redisAccessor.hget("test_hget_key2", "field1", ""));
        Assert.assertNull(redisAccessor.hget("test_hget_key2", "field1", null));
    }

    @Test
    public void testHset() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.hset("test_hset_key1", "field1", "value1", 3600);
        redisAccessor.hset("test_hset_key1", "field2", "value2", 3600);
        Assert.assertEquals("value1", redisAccessor.hget("test_hset_key1", "field1", "default_value"));
        Assert.assertEquals("value2", redisAccessor.hget("test_hset_key1", "field2", "default_value"));
    }

    @Test
    public void testHmset() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        redisAccessor.hmset("test_hmset_key", hash, 3600);
        Assert.assertEquals("value1", redisAccessor.hget("test_hmset_key", "field1", "default_value"));
        Assert.assertEquals("value2", redisAccessor.hget("test_hmset_key", "field2", "default_value"));
    }

    @Test
    public void testHdel() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.hset("test_hdel_key", "field1", "value1", 3600);
        redisAccessor.hset("test_hdel_key", "field2", "value2", 3600);
        Assert.assertEquals("value1", redisAccessor.hget("test_hdel_key", "field1", "default_value"));
        Assert.assertEquals("value2", redisAccessor.hget("test_hdel_key", "field2", "default_value"));
        redisAccessor.hdel("test_hdel_key", "field1");
        Assert.assertEquals("default_value", redisAccessor.hget("test_hdel_key", "field1", "default_value"));
        Assert.assertEquals("value2", redisAccessor.hget("test_hdel_key", "field2", "default_value"));
    }

    @Test
    public void testHincrby() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        Assert.assertEquals(5, redisAccessor.hincrby("test_hincrby_key", "field", 5, 0, 3600));
        Assert.assertEquals(10, redisAccessor.hincrby("test_hincrby_key", "field", 5, 0, 3600));
        Assert.assertEquals("10", redisAccessor.hget("test_hincrby_key", "field", "0"));
    }

    @Test
    public void testSadd() throws Exception {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor(prefix);
        redisAccessor.sadd("test_sadd_key", 3600, "value1", "value2", "value3");
        Set<String> values = redisAccessor.smembers("test_sadd_key");
        Set<String> expectedValues = new HashSet<>();
        expectedValues.add("value1");
        expectedValues.add("value2");
        expectedValues.add("value3");
        Assert.assertTrue(expectedValues.equals(values));

        values = redisAccessor.smembers("test_sadd_key2");
        Assert.assertTrue(values.isEmpty());
    }
}