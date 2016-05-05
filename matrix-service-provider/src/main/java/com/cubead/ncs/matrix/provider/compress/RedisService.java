package com.cubead.ncs.matrix.provider.compress;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.data.redis.core.RedisTemplate;

//@Service
public class RedisService {

    @Resource
    private RedisTemplate<String, Object> redisTemplate;

    public Object valueGet(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public void valueSet(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }

    public Object valueHGet(String key, String hashKey) {
        return redisTemplate.opsForHash().get(key, hashKey);
    }

    public void valueHSet(String key, String hashKey, Object value) {
        redisTemplate.opsForHash().put(key, hashKey, value);
    }

    public void valueSetExpire(String key, Object value, long timeout) {
        redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
    }

    public Object valueGetAndSet(String key, Object value) {
        return redisTemplate.opsForValue().getAndSet(key, value);
    }

    public boolean valueSetIfAbsent(String key, Object value) {
        return redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    public long lpush(String listName, Object value) {
        return redisTemplate.opsForList().leftPush(listName, value);
    }

    @SuppressWarnings("rawtypes")
    public long lpushAll(String listName, Collection values) {
        return redisTemplate.opsForList().leftPushAll(listName, values);
    }

    public Object rpop(String listName) {
        return redisTemplate.opsForList().rightPop(listName);
    }

    public long listSize(String listName) {
        return redisTemplate.opsForList().size(listName);
    }

    public long rpush(String listName, Object value) {
        return redisTemplate.opsForList().rightPush(listName, value);
    }

    public Object lindex(String listName, int index) {
        return redisTemplate.opsForList().index(listName, index);
    }

    public long setAdd(String setName, Object value) {
        return redisTemplate.opsForSet().add(setName, value);
    }

    public long setRemove(String setName, Object value) {
        return redisTemplate.opsForSet().remove(setName, value);
    }

    public Set<Object> setMembers(String setName) {
        return redisTemplate.opsForSet().members(setName);
    }

    public void hashMapPut(String mapName, String key, Object value) {
        redisTemplate.opsForHash().put(mapName, key, value);
    }

    public Object hashMapGet(String mapName, String key) {
        return redisTemplate.opsForHash().get(mapName, key);
    }

    public void hashMapDel(String mapName, String key) {
        redisTemplate.opsForHash().delete(mapName, key);
    }

    public Set<Object> zSetRangeByScore(String zSetName, long min, long max) {
        return redisTemplate.opsForZSet().rangeByScore(zSetName, min, max);
    }

    public void zSetRemoveRangeByScore(String zSetName, long min, long max) {
        redisTemplate.opsForZSet().removeRangeByScore(zSetName, min, max);
    }

    public void zSetAdd(String zSetName, Object value, double score) {
        redisTemplate.opsForZSet().add(zSetName, value, score);
    }

    public void redisOpsExpire(String key, long timeout, TimeUnit unit) {
        redisTemplate.expire(key, timeout, unit);
    }

    public long redisIncrement(String key) {
        return redisTemplate.opsForValue().increment(key, 1);
    }

    public long GetRedisIncrement(String key) {
        return redisTemplate.opsForValue().increment(key, 0);
    }

    public boolean tryGetCacheLock(String key, long timeout) {
        boolean isGetLock = false;
        long myTime = System.currentTimeMillis() + timeout + 1;
        if (valueSetIfAbsent(key, myTime)) {
            isGetLock = true;
        } else {
            Object oldTime = (Long) valueGet(key);
            if (oldTime != null) {
                if ((Long) oldTime < System.currentTimeMillis()) {
                    long setBeforeTime = (Long) valueGetAndSet(key, System.currentTimeMillis() + timeout + 1);
                    if (setBeforeTime < System.currentTimeMillis()) {
                        return true;
                    }
                }
            }
        }
        return isGetLock;
    }

    public void valueDel(String key) {
        redisTemplate.delete(key);
    }

    public void del(String key) {
        redisTemplate.delete(key);
    }
}