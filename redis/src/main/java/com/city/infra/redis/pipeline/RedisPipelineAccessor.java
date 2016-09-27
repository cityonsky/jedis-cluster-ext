package com.city.infra.redis.pipeline;

import com.city.infra.redis.RedisAccessorBase;
import com.city.infra.redis.RedisClusterContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by sky on 2016/9/20.
 *
 * 注意 RedisPipelineAccessor 是非线程安全的
 */
public class RedisPipelineAccessor extends RedisAccessorBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPipelineAccessor.class);

    private static final int MAX_QUEUE_LENGTH = 10000;

    private Queue<RedisCommand> commands;

    public RedisPipelineAccessor(RedisClusterContext context) {
        super("", context);
        commands = new LinkedList<>();
    }

    public String set(String prefix, String key, String value, int expireSeconds) {
        logWrite(prefix, key, value);
        addCommand(new RedisCommand<String>(Protocol.Command.SETEX, prefix + key, value, adjustExpireTime(expireSeconds)));
        if (shouldSync()) {
            sync();
        }
        return "";
    }

    public long delete(String prefix, String key) {
        logWrite(prefix, key, null);
        addCommand(new RedisCommand<Long>(Protocol.Command.DEL, prefix + key, null, 0));
        if (shouldSync()) {
            sync();
        }
        return 0;
    }

    public boolean sync() {
        final Queue<RedisCommand> imageForCommands = commands;
        commands = new LinkedList<>();

        if (CollectionUtils.isEmpty(imageForCommands)) {
            return true;
        }

        return new WriteCommandTemplateBoolean(StringUtils.EMPTY, null, 0) {
            @Override
            public Boolean write(JedisCluster client, String redisKey, int seconds) {
                RedisPipelineExecutor pipelineExecutor = new RedisPipelineExecutor(client);
                for (RedisCommand command : imageForCommands) {
                    command.response = pipelineExecutor.executeCommand(command);
                }
                pipelineExecutor.sync();

                // 使用 pipeline 执行失败的 Redis 命令，回退到普通方式执行。
                for (RedisCommand command : imageForCommands) {
                    try {
                        command.response.get();
                    } catch (Exception ignored) {
                        executeCommand(client, command);
                    }
                }

                return true;
            }
        }.run();
    }

    private void executeCommand(JedisCluster client, RedisCommand command) {
        try {
            switch (command.op) {
                case SETEX:
                    client.setex(command.key, command.expireTime, command.value);
                    break;
                case DEL:
                    client.del(command.key);
                    break;
                default:
                    throw new RuntimeException("Unsupported Redis command!");
            }
        } catch (Exception e) {
            LOGGER.warn("Execute Redis command failure!", e);
        }
    }

    private void addCommand(RedisCommand redisCommand) {
        commands.add(redisCommand);
    }

    private boolean shouldSync() {
        return commands.size() >= MAX_QUEUE_LENGTH;
    }

}
