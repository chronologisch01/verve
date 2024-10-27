package com.verve.coding_challenge.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


@RestController
@RequestMapping("/api/verve")
public class VerveController {

    private static final Logger logger = LoggerFactory.getLogger(VerveController.class);

    //private final ConcurrentMap<Long, Set<Integer>> minuteToIdsMap = new ConcurrentHashMap<>();

    @Autowired
    private RedisTemplate<String, Integer> redisTemplate;

    @Autowired
    private KafkaTemplate<String, Long> kafkaTemplate;

    @Autowired
    private WebClient.Builder webClientBuilder;

    @GetMapping("/accept")
    public Mono<String> accept(
            @RequestParam("id") int id,
            @RequestParam(value = "endpoint", required = false) String endpoint) {

        SetOperations<String, Integer> setOps = redisTemplate.opsForSet();
        String currentMinute = getCurrentMinute(0);
        // minuteToIdsMap.computeIfAbsent(currentMinute, k -> ConcurrentHashMap.newKeySet()).add(id);

        setOps.add(currentMinute, id);
        try {


            redisTemplate.expire(currentMinute, 5, TimeUnit.MINUTES);

            if (endpoint != null && !endpoint.isEmpty()) {
                //int uniqueCount = minuteToIdsMap.getOrDefault(currentMinute, Set.of()).size();
                Long uniqueCount = setOps.size(currentMinute);


                String urlWithParams = endpoint + "?count=" + uniqueCount;

                return webClientBuilder.build()
                        .post()
                        .uri(urlWithParams)
                        .exchangeToMono(response -> {
                            HttpStatusCode status = response.statusCode();
                            logger.info("HTTP Status Code from {}: {}", endpoint, status);
                            return response.bodyToMono(String.class);
                        })
                        .thenReturn("ok")
                        .onErrorResume(e -> {
                            logger.error("Error calling endpoint {}: {}", endpoint, e.getMessage());
                            return Mono.just("failed");
                        });
            }

            return Mono.just("ok");
        } catch (Exception e) {
            logger.error("Error processing request: {}", e.getMessage());
            return Mono.just("failed");
        }
    }


    @Scheduled(fixedRate = 60000)
    public void logMessage() {
        String currentMinute = getCurrentMinute(1) ;
        SetOperations<String, Integer> setOps = redisTemplate.opsForSet();
        Long count = setOps.size(currentMinute);
        logger.info("Unique request count for minute {}: {}", currentMinute, count);
        kafkaTemplate.send("unique-request-counts", currentMinute, count);
        redisTemplate.delete(currentMinute);
    }

    private String getCurrentMinute(int offset) {
        return String.valueOf((Instant.now().getEpochSecond() / 60) - offset);
    }
}
