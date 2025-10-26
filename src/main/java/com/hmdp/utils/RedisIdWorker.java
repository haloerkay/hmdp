package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    // 时间戳为当前时间-初始时间
    // 初始时间为2022年1月1日0时0分0秒对应的时间戳
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    // 序列号位数
    private static final int COUNT_BITS = 32;
    @Resource
    private  StringRedisTemplate stringRedisTemplate;


    // keyPrefix一般为业务字段名
    // 指定一段初始时间，当前时间 - 初始时间的时间戳为最终的时间戳
    // 业务字段 + 当前日期得到redis的键
    // 利用increment计数器得到count，时间戳拼接计数器得到最终的值
    // 此处的日期作用：参与键的构造、参与值中时间戳的构造
    public Long nextId(String keyPrefix){
        // 1. 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2. 生成序列号
        String  date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // icr用于标识业务，表示自增
        // increment方法的参数为redis中的键，如icr:order:2025:10:26
        // increment方法，如果键不存在，则会先将其初始化为0，然后再执行加一，返回给count
        // count的值是从1开始的
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // long型数字拼接，时间戳的二进制数字向左移动32位后再或上count
        return timestamp << COUNT_BITS | count;
    }


}
