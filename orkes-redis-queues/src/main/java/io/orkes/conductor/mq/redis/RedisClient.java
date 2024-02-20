package io.orkes.conductor.mq.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ZAddParams;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;

public interface RedisClient extends AutoCloseable {
    public Long zrem(final String key, final String... members);

    public Long hdel(final String key, final String... fields);

    public Response<Long> hset(final String key, final String field, final String value);

    public Response<Long> zadd(final String key, final double score, final String member);

    public Long zadd(final String key, final double score, final String member, final ZAddParams params);

    public Double zscore(final String key, final String member);

    public Long del(final String key);

    public Long zcard(final String key);

    public List<String> hmget(final String key, final String... fields);

    // Above is standalone
    // clustered and not covered above
    public Long zadd(final String key, final Map<String, Double> scoreMembers);

    public String hmset(final String key, final Map<String, String> hash);

    public List<String> evalsha(final String sha1, final List<String> keys, final List<String> args);

    public String hget(final String key, final String field);

    byte[] scriptLoad(byte[] script, byte[] bytes);

    default void close() {
    }



    static RedisClient of(Jedis jedis) {
        return getRedisClient(jedis, Jedis.class);
    }

    static RedisClient of(JedisCluster jedis) {
        return getRedisClient(jedis, JedisCluster.class);
    }

    private static RedisClient getRedisClient(Object jedis, final Class<?> jedisClass) {
      return (RedisClient) Proxy.newProxyInstance(RedisClient.class.getClassLoader(), new Class[]{RedisClient.class}, new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
              Method original = jedisClass.getMethod(method.getName(), method.getParameterTypes());
              return original.invoke(jedis, args);
          }
      });

    }

    private static RedisClient getRedisClient(JedisCluster jedis, final Class<?> jedisClass) {
        return (RedisClient) Proxy.newProxyInstance(RedisClient.class.getClassLoader(), new Class[]{RedisClient.class}, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if(method.getName().equals("close")) {
                    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                    if(isCalledFromJavaTryBlock(stackTrace)) {
                        // called from a try block, this is the result of a workaround
                        // to unify the interface of Jedis and JedisCluster
                        // so this call can be ignored
                        return null;
                    }
                }
                Method original = jedisClass.getMethod(method.getName(), method.getParameterTypes());
                return original.invoke(jedis, args);
            }
        });

    }

    private static boolean isCalledFromJavaTryBlock(StackTraceElement[] stackTrace) {
        return stackTrace[2].toString().contains("jdk.proxy");
    }
}

