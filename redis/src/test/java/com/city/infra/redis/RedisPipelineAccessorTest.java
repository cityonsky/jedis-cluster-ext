package com.city.infra.redis;

import com.city.infra.redis.pipeline.RedisPipelineAccessor;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class RedisPipelineAccessorTest extends TestCase {

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

        prefix = String.format("pipeline:%s:", UUID.randomUUID());
    }

    @Test
    public void testPipeline() throws Exception {
        RedisPipelineAccessor pipelineAccessor = clusterFactory.createRedisPipelineAccessor();
        pipelineAccessor.set(prefix, "key1", "value1", 3600);
        pipelineAccessor.set(prefix, "key2", "value2", 3600);
        pipelineAccessor.delete(prefix, "key1");

        RedisAccessor accessor = clusterFactory.createRedisAccessor(prefix);
        Assert.assertEquals("default_value", accessor.get("key1", "default_value"));
        Assert.assertEquals("default_value", accessor.get("key2", "default_value"));

        pipelineAccessor.sync();
        Assert.assertEquals("default_value", accessor.get("key1", "default_value"));
        Assert.assertEquals("value2", accessor.get("key2", "default_value"));
    }

}