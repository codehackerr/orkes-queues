package io.orkes.conductor.queue.dao;

import com.netflix.conductor.core.config.ConductorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.orkes.conductor.queue.config.QueueRedisProperties;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class DragonFlyStandaloneQueueDAOTest extends BaseQueueDAOTest{

    private static GenericContainer redis =
        new GenericContainer(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    private static JedisPool jedisPool;

    @BeforeAll
    public static void setUp() {

        redis.start();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(2);
        config.setMaxTotal(10);

        jedisPool = new JedisPool(config, redis.getHost(), redis.getFirstMappedPort());
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ConductorProperties conductorProperties = new ConductorProperties();
        QueueRedisProperties queueRedisProperties = new QueueRedisProperties(conductorProperties);
        redisQueue =
            new RedisQueueDAO(
                meterRegistry, jedisPool, queueRedisProperties, conductorProperties);
    }
}
