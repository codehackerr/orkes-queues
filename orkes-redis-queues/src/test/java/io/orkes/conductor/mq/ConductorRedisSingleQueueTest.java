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
package io.orkes.conductor.mq;

import io.orkes.conductor.mq.redis.single.ConductorRedisSingleQueue;
import io.orkes.conductor.mq.redis.single.RedisQueueMonitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisPool;

import java.time.Clock;

import static io.orkes.conductor.mq.QueueMessage.DEFAULT_PRIORITY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class ConductorRedisSingleQueueTest {
    @Mock
    private JedisPool jedisPool;

    @Mock
    private RedisQueueMonitor queueMonitor;

    @Test
    public void scoreIsMessagePriorityWhenNoTimeout() {
        ConductorRedisSingleQueue queue = new ConductorRedisSingleQueue("queueName", jedisPool, queueMonitor);
        double score = queue.getScore(Clock.systemDefaultZone().millis(), new QueueMessage("id", "payload", 0, 1));
        assertEquals(1d, score);
    }

    @Test
    public void scoreIsDefaultPriorityWhenNoTimeoutAndPriority() {
        ConductorRedisSingleQueue queue = new ConductorRedisSingleQueue("queueName", jedisPool, queueMonitor);
        long now = Clock.systemDefaultZone().millis();

        int score = (int) queue.getScore(now, new QueueMessage("id", "payload"));

        assertEquals(DEFAULT_PRIORITY, score);
    }
}
