package com.chen.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class TestRedis {

    @Autowired
    RedisTemplate template;

    @Autowired
    @Qualifier("ooxx")
    StringRedisTemplate stringRedisTemplate;

    @Autowired
    ObjectMapper mapper;


    public void testRedis() {
        RedisConnection conn = template.getConnectionFactory().getConnection();
        conn.set("name".getBytes(StandardCharsets.UTF_8), "chen".getBytes(StandardCharsets.UTF_8));

        HashOperations<String, Object, Object> hash = stringRedisTemplate.opsForHash();
        hash.put("sean","name","zhouzhilei");
        hash.put("sean","age","22");

        Person p = new Person();
        p.setName("zhangsan");
        p.setAge(16);
        Jackson2HashMapper jm = new Jackson2HashMapper(mapper, false);
        stringRedisTemplate.opsForHash().putAll("sean01",jm.toHash(p)); //serialize

        Map map = stringRedisTemplate.opsForHash().entries("sean01");

        Person per = mapper.convertValue(map, Person.class);
        System.out.println(per.getName()); //deserialize
        stringRedisTemplate.convertAndSend("ooxx","hello");//send a message to channel


        RedisConnection cc= stringRedisTemplate.getConnectionFactory().getConnection();

        //listen to the message
        cc.subscribe((message, pattern) -> {
            byte[] body = message.getBody();
            System.out.println(new String(body));
        }, "ooxx".getBytes(StandardCharsets.UTF_8));

        while (true) {
            stringRedisTemplate.convertAndSend("ooxx","hello  from myself ");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }



}
