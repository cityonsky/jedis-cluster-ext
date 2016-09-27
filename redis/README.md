# Infra Redis Client

Infra Redis client 基于 Jedis 实现了对 Redis 集群的访问封装。


## Redis Client 能做什么？
Infra Redis client 提供如下特性:

- 支持双机房集群 failover 方案
- 支持以 pipeline 方式访问 Redis 集群
- 支持 Redis 访问实时监测

## 怎么使用 Redis Client？

### 配置 maven 依赖:

```xml
<dependency>
    <groupId>com.city.infra</groupId>
    <artifactId>city-infra-redis</artifactId>
    <version>0.0.1</version>
    <type>jar</type>
    <scope>compile</scope>
</dependency>
```

### 构造集群工厂类

```java
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

        return clusterFactory;
    }
 
```

### 访问 Redis 集群
```java
RedisAccessor redisAccessor = clusterFactory.createRedisAccessor("prefix:");
redisAccessor.set("foo", "1", 3600);
String value = redisAccessor.get("foo", "0");
```

### pipeline 访问方式
pipeline 是 Redis 提供的一种批量处理命令机制，可以一次发送多个命令而不用同步等待响应结果，可以显著提升 Redis 访问性能，非常适合离线批量导入数据的业务。

Jedis 对 Redis 集群不支持 pipeline 方式访问，Infra Redis client 通过 Java 反射机制复用了 Jedis 对 Redis 单实例 pipeline 实现，在 Redis 集群节点变更不频繁的情况下，可以提供很好的访问性能。

pipeline 使用代码示例如下:
```java
RedisPipelineAccessor redisPipelineAccessor = clusterFactory.createRedisPipelineAccessor();
redisPipelineAccessor.set("pipe:", "foo", "1", 3600);
redisPipelineAccessor.set("pipe:", "bar", "2", 3600);
redisPipelineAccessor.sync();
```

### 监控 Redis 使用情况

在具体业务场景中，往往会对 Redis 的使用做一些规范，比如单个 key-value 的长度不能超过一定阈值，以防止对 Redis 的访问产生抖动。通过实现 RedisMonitor 接口，可以监控所有 Redis 访问情况。

为了使用方便，可以继承 RedisMonitorBase 类，选择业务感兴趣的事件监测，示例代码如下：

```java
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
    
```

在构造 Redis 集群工厂后，创建 Redis 访问对象前，设置自定义的监控对象到 Redis 集群工厂中：

```java
public static RedisClusterFactory buildFactory() {
        RedisClusterFactory clusterFactory = new RedisClusterFactory();

        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7639));
        jedisClusterNodes.add(new HostAndPort("10.100.1.1", 7640));

        Set<HostAndPort> standbyJedisClusterNodes = new HashSet<HostAndPort>();
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7543));
        standbyJedisClusterNodes.add(new HostAndPort("10.100.1.2", 7544));

        clusterFactory.setJedisClusterNodes(jedisClusterNodes);
        clusterFactory.setStandbyJedisClusterNodes(standbyJedisClusterNodes);
        clusterFactory.setRedisMonitor(new CustomMonitor());  //  设置访问监控对象

        return clusterFactory;
    }
```