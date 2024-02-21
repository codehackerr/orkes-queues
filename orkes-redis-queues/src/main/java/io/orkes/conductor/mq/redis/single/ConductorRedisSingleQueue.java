/*
 * Copyright 2022 Orkes, Inc.
 * <p>
 * Licensed under the Orkes Community License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * https://github.com/orkes-io/licenses/blob/main/community/LICENSE.txt
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.orkes.conductor.mq.redis.single;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.orkes.conductor.mq.redis.ConductorRedisQueue;
import io.orkes.conductor.mq.redis.RedisClient;

import io.orkes.conductor.mq.QueueMessage;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.Pipeline;

@Slf4j
public class ConductorRedisSingleQueue extends ConductorRedisQueue {

    private final JedisPoolAbstract jedisPool;

    public ConductorRedisSingleQueue(String queueName, JedisPoolAbstract jedisPool, RedisQueueMonitor queueMonitor1) {
        super(queueName, queueMonitor1);
        this.jedisPool = jedisPool;
        log.info("ConductorRedisQueue started serving {}", queueName);
    }
    public ConductorRedisSingleQueue(String queueName, JedisPoolAbstract jedisPool) {
        super(queueName, new RedisQueueMonitor(jedisPool, queueName));
        this.jedisPool = jedisPool;
        log.info("ConductorRedisQueue started serving {}", queueName);
    }

    // TODO: Unify the interface and pull to ConductorRedisQueue
    // With that this Object will no longer be required
    @Override
    public void push(List<QueueMessage> messages) {

        Map<String, Double> scoreMembers = new HashMap<>(messages.size());
        Map<String, String> payloads = new HashMap<>();

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipe = jedis.pipelined();
            setScoreAndPayload(messages, scoreMembers, payloads);
            pipe.zadd(queueName, scoreMembers);
            if (!payloads.isEmpty()) {
                pipe.hmset(payloadKey, payloads);
            }

            pipe.sync();
            pipe.close();
        }
    }

    @Override
    protected RedisClient redis() {
        return RedisClient.of(jedisPool.getResource());
    }
}
