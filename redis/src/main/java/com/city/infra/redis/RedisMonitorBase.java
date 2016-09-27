package com.city.infra.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sky on 2016/9/21.
 */
public class RedisMonitorBase implements RedisMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMonitorBase.class);

    @Override
    public void onRead(String prefix, String key) {
        LOGGER.debug("Reading redis prefix=[{}], key=[{}]", prefix, key);
    }

    @Override
    public void onWrite(String prefix, String key, String value) {
        LOGGER.debug("Writing redis prefix=[{}], key=[{}], value=[{}]", prefix, key, value);
    }

    @Override
    public void handleReadError(String prefix, String key, Exception e) {
        LOGGER.warn("Read redis error. prefix=[{}], key=[{}], exception=[{}]", prefix, key, e);
    }

    @Override
    public void handleWriteError(String prefix, String key, Exception e) {
        LOGGER.warn("Write redis error. prefix=[{}], key=[{}], exception=[{}]", prefix, key, e);
    }
}
