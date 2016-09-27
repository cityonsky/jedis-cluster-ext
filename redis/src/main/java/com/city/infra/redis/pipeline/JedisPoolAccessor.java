package com.city.infra.redis.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.lang.reflect.Field;

/**
 * Created by sky on 2016/9/22 0022.
 */
public class JedisPoolAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolAccessor.class);

    private static final Field FIELD_CONNECTION_HANDLER;
    private static final Field FIELD_CACHE;
    static {
        FIELD_CONNECTION_HANDLER = getField(JedisCluster.class, "connectionHandler");
        FIELD_CACHE = getField(JedisClusterConnectionHandler.class, "cache");
    }

    private JedisCluster client;

    public JedisPoolAccessor(JedisCluster client) {
        this.client = client;
    }

    public JedisPool getJedisPool(String key) {
        try {
            JedisSlotBasedConnectionHandler connectionHandler = getValue(client, FIELD_CONNECTION_HANDLER);
            JedisClusterInfoCache clusterInfoCache = getValue(connectionHandler, FIELD_CACHE);
            int slot = JedisClusterCRC16.getSlot(key);
            return clusterInfoCache.getSlotPool(slot);
        } catch (Exception e) {
            LOGGER.warn("Get Jedis pool failure!", e);
        }
        return null;
    }

    private static Field getField(Class<?> cls, String fieldName) {
        try {
            Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Can not find or access field '" + fieldName + "' from " + cls.getName(), e);
        } catch (SecurityException e) {
            throw new RuntimeException("Can not find or access field '" + fieldName + "' from " + cls.getName(), e);
        }
    }

    private static <T> T getValue(Object obj, Field field) throws IllegalAccessException {
        return (T)field.get(obj);
    }
}
