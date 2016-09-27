package com.city.infra.redis;

import org.apache.commons.collections.SetUtils;
import redis.clients.jedis.JedisCluster;

import java.util.Map;
import java.util.Set;

/**
 * Created by sky on 2016/9/20.
 */
public class RedisAccessor extends RedisAccessorBase {

    public RedisAccessor(String prefix, RedisClusterContext context) {
        super(prefix, context);
    }

    public String get(final String key, final String defaultValue) {
        return new ReadCommandTemplate<String>(key, defaultValue) {
            @Override
            public String read(JedisCluster client, String redisKey) {
                String value = client.get(redisKey);
                if (value == null) {
                    return defaultValue;
                } else {
                    return value;
                }
            }
        }.run();
    }

    public boolean set(final String key, final String value, final int expireSeconds) {
        return new WriteCommandTemplateBoolean(key, value, expireSeconds) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                client.setex(redisKey, seconds, value);
                return true;
            }
        }.run();
    }

    public boolean delete(final String key) {
        return new DeleteCommandTemplate(key) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                return client.del(redisKey) > 0;
            }
        }.run();
    }

    public long incrby(String key, final int increment, final long defaultValue, final int expireSeconds) {
        return new WriteCommandTemplate<Long>(key, null, expireSeconds, defaultValue) {
            @Override
            public Long write(JedisCluster client, String redisKey, int seconds) {
                long value = client.incrBy(redisKey, increment);
                client.expire(redisKey, seconds);
                return value;
            }
        }.run();
    }

    public String hget(final String key, final String field, final String defaultValue) {
        return new ReadCommandTemplate<String>(key, defaultValue) {
            @Override
            public String read(JedisCluster client, String redisKey) {
                String value = client.hget(redisKey, field);
                if (value == null) {
                    return defaultValue;
                } else {
                    return value;
                }
            }
        }.run();
    }

    public boolean hset(final String key, final String field, final String value, final int expireSeconds) {
        return new WriteCommandTemplateBoolean(key, value, expireSeconds) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                client.hset(redisKey, field, value);
                client.expire(redisKey, seconds);
                return true;
            }
        }.run();
    }

    public boolean hmset(final String key, final Map<String, String> hash, final int expireSeconds) {
        return new WriteCommandTemplateBoolean(key, null, expireSeconds) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                client.hmset(redisKey, hash);
                client.expire(redisKey, seconds);
                return true;
            }
        }.run();
    }

    public boolean hdel(final String key, final String field) {
        return new DeleteCommandTemplate(key) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                return client.hdel(redisKey, field) > 0;
            }
        }.run();
    }

    public long hincrby(final String key, final String field, final int increment, final long defaultValue, final int expireSeconds) {
        return new WriteCommandTemplate<Long>(key, String.valueOf(increment), expireSeconds, defaultValue) {
            @Override
            public Long write(JedisCluster client, String redisKey, int seconds) {
                long value = client.hincrBy(redisKey, field, increment);
                client.expire(redisKey, seconds);
                return value;
            }
        }.run();
    }

    public Set<String> smembers(final String key) {
        return new ReadCommandTemplate<Set<String>>(key, SetUtils.EMPTY_SET) {
            @Override
            public Set<String> read(JedisCluster client, String redisKey) {
                return client.smembers(redisKey);
            }
        }.run();
    }

    public long sadd(final String key, final int expireSeconds, final String... members) {
        return new WriteCommandTemplate<Long>(key, null, expireSeconds, -1L) {
            @Override
            public Long write(JedisCluster client, String redisKey, int seconds) {
                long ret = client.sadd(redisKey, members);
                client.expire(redisKey, seconds);
                return ret;
            }
        }.run();
    }

}
