package com.city.infra.redis.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sky on 2016/9/22.
 */
public class RedisPipelineExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPipelineAccessor.class);

    private JedisPoolAccessor jedisPoolAccessor;

    private Map<JedisPool, JedisPipelineUnit> jedisPipelineUnits = new HashMap<>();

    public RedisPipelineExecutor(JedisCluster client) {
        jedisPoolAccessor = new JedisPoolAccessor(client);
    }

    public <T> Response<T> executeCommand(RedisCommand<T> command) {
        try {
            JedisPool pool = jedisPoolAccessor.getJedisPool(command.key);
            if (pool == null) {
                return null;
            }
            JedisPipelineUnit jedisPipeline = jedisPipelineUnits.get(pool);
            if (jedisPipeline == null) {
                jedisPipeline = new JedisPipelineUnit(pool);
                jedisPipelineUnits.put(pool, jedisPipeline);
            }

            Pipeline pipeline = jedisPipeline.getPipeline();
            switch (command.op) {
                case SETEX:
                    return (Response<T>) pipeline.setex(command.key, command.expireTime, command.value);
                case DEL:
                    return (Response<T>) pipeline.del(command.key);
                default:
                    throw new RuntimeException("Unsupported Redis command = [!" + command.op + "]");
            }
        } catch (Throwable e) {
            LOGGER.warn("Execute pipeline command failure! ", e);
        }
        return null;
    }

    public void sync() {
        for (JedisPipelineUnit jedisPipelineUnit : jedisPipelineUnits.values()) {
            try {
                Pipeline pipeline = jedisPipelineUnit.getPipeline();
                pipeline.sync();
            } catch (Exception e) {
                LOGGER.warn("Pipeline sync failure!", e);
            } finally {
                jedisPipelineUnit.close();
            }
        }
        jedisPipelineUnits.clear();
    }
}
