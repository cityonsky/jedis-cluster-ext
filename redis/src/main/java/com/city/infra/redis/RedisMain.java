package com.city.infra.redis;

import com.city.infra.redis.pipeline.RedisPipelineAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sky on 2016/9/20.
 */
public class RedisMain {

    public static class CustomMonitor extends RedisMonitorBase {

        private static final Logger LOGGER = LoggerFactory.getLogger(CustomMonitor.class);

        private static final int MAX_VALUE_LEN = 1024 * 1024;

        @Override
        public void onWrite(String prefix, String key, String value) {
            if (value.length() > MAX_VALUE_LEN) {
                LOGGER.warn("Too large redis value! prefix=[{}], key=[{}], value=[{}]", prefix, key, value);
            }
        }
    }

    public static RedisClusterFactory buildFactory() {
        RedisClusterFactory clusterFactory = new RedisClusterFactory();

        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7639));
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7640));

        Set<HostAndPort> standbyJedisClusterNodes = new HashSet<HostAndPort>();
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7543));
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7544));

        clusterFactory.setJedisClusterNodes(jedisClusterNodes);  //  设置主集群
        clusterFactory.setStandbyJedisClusterNodes(standbyJedisClusterNodes);  //  设置备份集群
        clusterFactory.setRedisMonitor(new CustomMonitor());

        return clusterFactory;
    }

    public static void testRedisAccessor(RedisClusterFactory clusterFactory) {
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor("accessor:");
        redisAccessor.set("foo", "1", 3600);
        String value = redisAccessor.get("foo", "0");
        System.out.println(value);
    }

    public static void testRedisPipelineAccessor(RedisClusterFactory clusterFactory) {
        RedisPipelineAccessor redisPipelineAccessor = clusterFactory.createRedisPipelineAccessor();
        redisPipelineAccessor.set("pipe:", "foo", "1", 3600);
        redisPipelineAccessor.set("pipe:", "bar", "2", 3600);
        redisPipelineAccessor.sync();
        RedisAccessor redisAccessor = clusterFactory.createRedisAccessor("pipe:");
        String value = redisAccessor.get("foo", "0");
        System.out.println(value);
    }

    public static void main(String[] args) {
        RedisClusterFactory clusterFactory = buildFactory();
        testRedisAccessor(clusterFactory);
        testRedisPipelineAccessor(clusterFactory);
    }
}
