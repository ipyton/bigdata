package com.chen.redis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class RedisApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(RedisApplication.class, args);
        TestRedis redis = run.getBean(TestRedis.class);
        redis.testRedis();


    }

}
