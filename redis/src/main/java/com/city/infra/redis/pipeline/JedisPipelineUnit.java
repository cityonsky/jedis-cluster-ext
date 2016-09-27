package com.city.infra.redis.pipeline;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.io.Closeable;

/**
 * Created by sky on 2016/9/22 .
 */
public class JedisPipelineUnit {

    private Jedis jedis;

    private Pipeline pipeline;

    public JedisPipelineUnit(JedisPool pool) {
        this.jedis = pool.getResource();
        this.pipeline = jedis.pipelined();
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void close() {
        closeQuietly(pipeline);
        closeQuietly(jedis);
        pipeline = null;
        jedis = null;
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }
}
