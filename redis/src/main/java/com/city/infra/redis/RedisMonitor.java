package com.city.infra.redis;

/**
 * Created by sky on 2016/9/21.
 */
public interface RedisMonitor {

    void onRead(String prefix, String key);

    void onWrite(String prefix, String key, String value);

    void handleReadError(String prefix, String key, Exception e);

    void handleWriteError(String prefix, String key, Exception e);

}
