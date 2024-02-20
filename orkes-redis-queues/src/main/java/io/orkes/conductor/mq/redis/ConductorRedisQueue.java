package io.orkes.conductor.mq.redis;

import io.orkes.conductor.mq.ConductorQueue;
import io.orkes.conductor.mq.QueueMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import redis.clients.jedis.params.ZAddParams;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class ConductorRedisQueue implements ConductorQueue {
    protected abstract RedisClient redis();

    protected final Clock clock;

    protected String queueName;

    protected final String payloadKey;

    protected final QueueMonitor queueMonitor;

    public ConductorRedisQueue(String queueName, QueueMonitor queueMonitor) {
        this.clock = Clock.systemDefaultZone();
        this.queueName = queueName;
        this.payloadKey = queueName + "_payload";
        this.queueMonitor = queueMonitor;
    }

    @Override
    public void flush() {
        try (RedisClient jedis = redis()) {
            jedis.del(queueName);
            jedis.del(payloadKey);
        }
    }

    @Override
    public boolean ack(String messageId) {
        Long removed;
        try (RedisClient redisClient = redis()) {
            removed = redisClient.zrem(queueName, messageId);
            redisClient.hdel(payloadKey, messageId);
        }
        return removed > 0;
    }

    @Override
    public void remove(String messageId) {

        try (RedisClient redis = redis()) {
            redis.zrem(queueName, messageId);
            redis.hdel(payloadKey, messageId);
        }

        return;
    }

    @Override
    public List<QueueMessage> pop(int count, int waitTime, TimeUnit timeUnit) {
        List<QueueMessage> messages = queueMonitor.pop(count, waitTime, timeUnit);
        if (messages.isEmpty()) {
            return messages;
        }
        String[] messageIds = messages.stream().map(QueueMessage::getId).toArray(String[]::new);
        List<String> payloads = getPayloads(messageIds);
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).setPayload(payloads.get(i));
        }
        return messages;
    }


    @Override
    public String getName() {
        return queueName;
    }


    private List<String> getPayloads(String[] messageIds) {
        try (RedisClient redis = redis()) {
            List<String> payloads = redis.hmget(payloadKey, messageIds);
            return payloads;
        }
    }

    @Override
    public boolean setUnacktimeout(String messageId, long unackTimeout) {
        double score = clock.millis() + unackTimeout;
        try (RedisClient redis = redis()) {
            ZAddParams params =
                ZAddParams.zAddParams()
                    .xx() // only update, do NOT add
                    .ch(); // return modified elements count
            Long modified = redis.zadd(queueName, score, messageId, params);
            return modified != null && modified > 0;
        }
    }

    @Override
    public boolean exists(String messageId) {
        try (RedisClient redis = redis()) {
            Double score = redis.zscore(queueName, messageId);
            if (score != null) {
                return true;
            }
        }
        return false;
    }




    @Override
    public QueueMessage get(String messageId) {

        try (RedisClient redis = redis()) {
            Double score = redis.zscore(queueName, messageId);
            if (score == null) {
                return null;
            }
            int priority =
                new BigDecimal(score).remainder(BigDecimal.ONE).multiply(HUNDRED).intValue();
            String payload = redis.hget(payloadKey, messageId);
            QueueMessage message =
                new QueueMessage(messageId, payload, score.longValue(), priority);
            return message;
        }
    }

    @Override
    public long size() {
        try (RedisClient redis = redis()) {
            return redis.zcard(queueName);
        }
    }

    @Override
    public int getQueueUnackTime() {
        return queueMonitor.getQueueUnackTime();
    }

    @Override
    public void setQueueUnackTime(int queueUnackTime) {
        queueMonitor.setQueueUnackTime(queueUnackTime);
    }

    @Override
    public String getShardName() {
        return null;
    }

    protected void setScoreAndPayload(List<QueueMessage> messages, Map<String, Double> scoreMembers, Map<String, String> payloads) {

        // it will be neate to split this method into two methods
        // 1. getScores(messages)
        //      messages.map(this::toScore).collect(Collectors.toMap())
        // AND
        // 2. getPayloads(messages)
        //      messages.map(this::toPayload).collect(Collectors.toMap())
        //
        // However the implementation will be O(2 * n)
        //
        // The below implementation is O(n) and is slightly more efficient (theoretically :))
        //
        // Practical question is, What is the typical value of n?

        long now = clock.millis();

        messages.stream()
            .map((m) ->
                Triple.of(m.getId(), getScore(now, m), m.getPayload())
            ).forEach((t) -> {
                String messageId = t.getLeft();
                Double score = t.getMiddle();
                String payload = t.getRight();

                scoreMembers.put(messageId, score);

                if (StringUtils.isNotBlank(payload)) {
                    payloads.put(messageId, payload);
                }
            });
    }


}
