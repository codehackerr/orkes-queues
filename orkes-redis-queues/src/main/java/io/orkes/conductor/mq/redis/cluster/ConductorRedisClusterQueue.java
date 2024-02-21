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
package io.orkes.conductor.mq.redis.cluster;

import io.orkes.conductor.mq.QueueMessage;
import io.orkes.conductor.mq.redis.ConductorRedisQueue;
import io.orkes.conductor.mq.redis.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ConductorRedisClusterQueue extends ConductorRedisQueue {
    private final JedisCluster jedis;

    public ConductorRedisClusterQueue(String queueName, JedisCluster jedisCluster) {
        super(queueName, new ClusteredQueueMonitor(jedisCluster, queueName));
        this.jedis = jedisCluster;
        log.info("ConductorRedisClusterQueue started serving {}", queueName);
    }

    // TODO: Unify the interface and pull to ConductorRedisQueue
    // With that this Object will no longer be required
    @Override
    public void push(List<QueueMessage> messages) {
        Map<String, Double> scoreMembers = new HashMap<>(messages.size());
        Map<String, String> payloads = new HashMap<>();
        setScoreAndPayload(messages, scoreMembers, payloads);
        jedis.zadd(queueName, scoreMembers);
        if (!payloads.isEmpty()) {

            // changing to hset because of deprecation https://redis.io/commands/hmset/

            jedis.hset(payloadKey, payloads);
        }
    }

    @Override
    protected RedisClient redis() {
        return RedisClient.of(this.jedis);
    }
}