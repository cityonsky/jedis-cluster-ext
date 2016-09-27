package com.city.infra.redis;

import com.city.infra.redis.pipeline.RedisPipelineAccessor;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

/**
 * Created by sky on 2016/9/20.
 */
public class RedisClusterFactory {

    private final static int CLIENT_POOLSIZE = 40;

    private final static int CLUSTER_TIMEOUT = 2000;

    private JedisPoolConfig poolConfig = null;

    private JedisCluster jedisCluster;

    private JedisCluster standyJedisCluster;

    private Set<HostAndPort> jedisClusterNodes;

    private Set<HostAndPort> standbyJedisClusterNodes;

    private RedisMonitor redisMonitor = null;

    public RedisClusterFactory() {
    }

    public RedisAccessor createRedisAccessor(String prefix) {
        return new RedisAccessor(prefix, getRedisClusterContext());
    }

    public RedisPipelineAccessor createRedisPipelineAccessor() {
        return new RedisPipelineAccessor(getRedisClusterContext());
    }

    public void setJedisClusterNodes(Set<HostAndPort> jedisClusterNodes) {
        this.jedisClusterNodes = jedisClusterNodes;
    }

    public void setStandbyJedisClusterNodes(Set<HostAndPort> standbyJedisClusterNodes) {
        this.standbyJedisClusterNodes = standbyJedisClusterNodes;
    }

    public void setRedisMonitor(RedisMonitor monitor) {
        this.redisMonitor = monitor;
    }

    private RedisClusterContext getRedisClusterContext() {
        RedisClusterContext context = new RedisClusterContext();
        context.setJedisCluster(getCluster());
        context.setStandyJedisCluster(getStandyCluster());
        context.setMonitor(getRedisMonitor());
        return context;
    }

    private JedisCluster getCluster() {
        if (null == jedisCluster) {
            synchronized (this) {
                if (null == jedisCluster) {
                    jedisCluster = new JedisCluster(getJedisClusterNodes(), CLUSTER_TIMEOUT, getJedisPoolConfig());
                }
            }
        }
        return jedisCluster;
    }

    private JedisCluster getStandyCluster() {
        if (null == standyJedisCluster) {
            synchronized (this) {
                if (null == standyJedisCluster) {
                    standyJedisCluster = new JedisCluster(getStandbyJedisClusterNodes(), CLUSTER_TIMEOUT, getJedisPoolConfig());
                }
            }
        }
        return standyJedisCluster;
    }

    private JedisPoolConfig getJedisPoolConfig() {
        if(poolConfig == null) {
            poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(CLIENT_POOLSIZE);
        }
        return poolConfig;
    }

    private Set<HostAndPort> getJedisClusterNodes() {
        return jedisClusterNodes;
    }

    private Set<HostAndPort> getStandbyJedisClusterNodes() {
        return standbyJedisClusterNodes;
    }

    private RedisMonitor getRedisMonitor() {
        return redisMonitor;
    }

}
