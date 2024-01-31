package com.mongodb.schemaversioning.service;


import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Service
public class BatchUpdateService {
    
    @Scheduled(fixedDelay = 2000)
    public void scheduleFixedDelayTask() {
    }

    @PostConstruct
    private void startChangeStream() {
        /*CompletableFuture.supplyAsync(() -> {
            return null;
        });*/
    }
}

