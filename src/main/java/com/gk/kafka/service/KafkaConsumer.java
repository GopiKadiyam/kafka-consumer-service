package com.gk.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class KafkaConsumer {
    @Autowired
    private MessageService messageService;

    @KafkaListener(topics = "${msgs.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        String batchUUID = "BATCH-ID-" + UUID.randomUUID().toString() + " :: ";
        log.info("--- {} NO OF MESSAGES RECEIVED FOR BATCH {} ---", batchUUID, records.size());
        if (records.size() != 0) {
            records.forEach(rec -> {
                String msgKey = rec.key();
                log.info("{} processing message for key {} with timestamp {}", batchUUID, msgKey, rec.timestamp());
                messageService.processMessage(rec.value(), batchUUID, msgKey);
            });
        }
        // Manually acknowledge the batch after processing
        acknowledgment.acknowledge();
        log.info("--- {} {} messages got processed and stored in MongoDB ---", batchUUID, records.size());
    }

}
