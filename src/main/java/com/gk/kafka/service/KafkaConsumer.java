package com.gk.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gk.kafka.entities.MessageDocument;
import com.gk.kafka.repository.MessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class KafkaConsumer {

    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private ObjectMapper objectMapper;
    // Define the required timestamp (March 2nd, 2025, 18:00 UTC)
    long startTimestamp = LocalDateTime.of(2025, 3, 5, 0, 0)
            .atZone(ZoneId.of("UTC"))
            .toInstant()
            .toEpochMilli();
    @KafkaListener(topicPartitions = @TopicPartition(topic = "${msgs.kafka.topic.name}", partitions = {"5"}),
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumePartition0(ConsumerRecord<String, String> record) {
        String batchUUID = "BATCH-ID-" + UUID.randomUUID().toString() + " :: ";
        //log.info("--- {} Received message from Partition 0: Key = {}, Timestamp = {} ---", batchUUID, record.key(), record.timestamp());
//        processMessage(record.value(), batchUUID, record.key(), record.timestamp());

        // Process only if the message timestamp is after or equal to startTimestamp
        if (record.timestamp() >= startTimestamp) {
            processMessage(record.value(), batchUUID, record.key(), record.timestamp());
        }
    }

    public void processMessage(String message, String batchUUID, String kafkaMsgKey, long timestamp) {
        try {
            message = message.replaceAll("[\\r\\n]+", "").trim();
            JsonNode jsonNode = objectMapper.readTree(message);
            String messageId = jsonNode.get("Id").asText();
            if (jsonNode.has("Status")) {
                //handleUpdateMessage(jsonNode, batchUUID, kafkaMsgKey, messageId, timestamp);
            } else {
                handleInsertMessage(jsonNode, batchUUID, kafkaMsgKey, messageId, timestamp);
            }
        } catch (Exception e) {
            // TODO write logic to To Store message into DB
            log.info("{} PROCESS_MSG - kafkaMsgKey {} - timestamp {} - #PROCESS_ERR  ", batchUUID, kafkaMsgKey, timestamp);
        }
    }

    private void handleInsertMessage(JsonNode jsonNode, String batchUUID, String kafkaMsgKey, String messageId, long timestamp) {
        try {

            // Check if document already exists
            Optional<MessageDocument> existingMessageId = messageRepository.findByMessageId(messageId);
            if (existingMessageId.isPresent()) {
                log.info("{} INSERT_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - #DUPINSERT_ERR  ", batchUUID, kafkaMsgKey, messageId, timestamp);
            }
            if (existingMessageId.isEmpty()) {
                MessageDocument messageDoc = new MessageDocument();
                String uniqueId;
                do {
                    uniqueId = new ObjectId().toHexString();  // Generate ObjectId as String
                } while (messageRepository.existsById(uniqueId));  // Repeat if ID already exists
                messageDoc.setId(uniqueId); // Generate a valid ObjectId and store as String
                messageDoc.setMessageId(messageId);
                messageDoc.setTenantId(jsonNode.get("TenantId").asText());
                String receivedAt = jsonNode.get("ReceivedAt").asText();
                // Define formatter for ISO-8601 date-time
                DateTimeFormatter isoFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                // Parse input string to ZonedDateTime (assumed to be in IST)
                ZonedDateTime istDateTime = ZonedDateTime.parse(receivedAt, isoFormatter);
                // Convert IST to GMT (UTC)
                Instant instant = istDateTime.withZoneSameInstant(ZoneId.of("UTC")).toInstant();

                // Convert to GMT (UTC) and get instant
                messageDoc.setReceivedAt(Date.from(instant));

                JsonNode request = jsonNode.get("Request");
                messageDoc.setFrom(request.get("from").asText());
                messageDoc.setTo(request.get("to").asText());
                messageDoc.setCountry(request.get("country").asText());
                String msgBody = request.get("body").asText();
                messageDoc.setBody(msgBody);
                messageDoc.setTemplateId(request.get("templateId").asText());
                messageDoc.setEntityId(request.get("entityId").asText());
                messageDoc.setMessageType(request.get("messageType").asInt());
                messageDoc.setCustomId(request.get("customId").asText());
                JsonNode metadataJson = request.get("metadata");
                messageDoc.setMetadata(objectMapper.convertValue(metadataJson, Map.class));
                messageDoc.setFlash(request.get("flash").asBoolean());
                //ignore Need to check map for "serviceType": 0

                messageDoc.setBatchId(jsonNode.get("BatchId").asText());
                JsonNode parameterJson = request.get("Parameters");
                messageDoc.setParameters(parameterJson != null ? objectMapper.convertValue(parameterJson, Map.class) : Collections.emptyMap());
                messageDoc.setUnits(getUnits(msgBody));
                messageRepository.save(messageDoc);
                log.info("{} INSERT_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - INSERTED_INTO_DB  ", batchUUID, kafkaMsgKey, messageId, timestamp);

            }

        } catch (Exception e) {
            log.info("{} INSERT_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - #INSERTPARSE_ERR  ", batchUUID, kafkaMsgKey, messageId, timestamp);
        }
    }

    private int getUnits(String msgBody) {
        // Check if the message contains only ASCII characters
        // Optimized check: If any character is non-ASCII, mark it as non-ASCII
        if (msgBody == null || msgBody.equalsIgnoreCase(""))
            return 1;
        boolean isNonAscii = msgBody.chars().anyMatch(c -> c >= 128);

        int unit;
        if (!isNonAscii) {
            // ASCII logic
            unit = (msgBody.length() <= 160) ? 1 : (int) Math.ceil((double) msgBody.length() / 153);
        } else {
            // Non-ASCII logic
            unit = (msgBody.length() <= 70) ? 1 : (int) Math.ceil((double) msgBody.length() / 67);
        }
        return unit;
    }

    private void handleUpdateMessage(JsonNode jsonNode, String batchUUID, String kafkaMsgKey, String messageId, long timestamp) {
        try {
            // Check if document already exists
            Optional<MessageDocument> existingMessageId = messageRepository.findByMessageId(messageId);

            if (existingMessageId.isPresent()) {
                MessageDocument messageDoc = existingMessageId.get();
                String status = jsonNode.get("Status").asText();
                String partnerMsgId = jsonNode.get("PartnerMessageId").asText();
                String smsc = jsonNode.get("Smscid").asText();
                String serviceId = jsonNode.get("ServiceId").asText();
                String doneDateStr = extractFieldFromSource(status, "done date:(\\d{12})");
                Date doneDate = null;
                String statusCode = extractFieldFromSource(status, "err:([^\\s]+)");
                if (doneDateStr != null) {
                    doneDate = convertToDateObject(doneDateStr, batchUUID, kafkaMsgKey);
                }
                messageDoc.setPartnerMessageId(partnerMsgId);
                messageDoc.setSmsc(smsc);
                messageDoc.setServiceId(serviceId);
                messageDoc.setDlrReceivedAt(doneDate);
                messageDoc.setProcessedAt(doneDate);
                messageDoc.setStatus(statusCode);
                messageRepository.save(messageDoc);
                log.info("{} UPDATE_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - UPDATED_INTO_DB  ", batchUUID, kafkaMsgKey, messageId, timestamp);

            } else {
                //TODO put entry to error collection
                log.info("{} UPDATE_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - #UPDATEONNONEXISTINGREC_ERR  ", batchUUID, kafkaMsgKey, messageId, timestamp);
            }
        } catch (Exception e) {
            log.info("{} UPDATE_MSG - kafkaMsgKey {} - MessageId {} - timestamp {} - #UPDATEPARSE_ERR  ", batchUUID, kafkaMsgKey, messageId, timestamp);
        }
    }

    private String extractFieldFromSource(String source, String regexPattern) {
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(source);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private Date convertToDateObject(String yymmddHHmmSS, String batchUUID, String kafkaMsgKey) {
        try {
            // Define the input formatter
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyMMddHHmmss");
            // Parse input string to LocalDateTime (assuming it's in IST)
            LocalDateTime localDateTime = LocalDateTime.parse(yymmddHHmmSS, inputFormatter);

            // Convert LocalDateTime from IST to Instant in GMT
            Instant instant = localDateTime.atZone(ZoneId.of("Asia/Kolkata")) // Convert from IST
                    .withZoneSameInstant(ZoneId.of("UTC")) // Convert to GMT
                    .toInstant();

            // Convert to java.util.Date
            return Date.from(instant);
        } catch (Exception e) {
            log.info("{} - kafkaMsgKey {} - Error while converting doneDate from format yyMMddHHmmss to Date object ", batchUUID, kafkaMsgKey, e);
            log.info("input for date conversion is {}", yymmddHHmmSS);
            return null;
        }
    }


//    @KafkaListener(id = "gkListener", topics = "${msgs.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
//    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
//        String batchUUID = "BATCH-ID-" + UUID.randomUUID().toString() + " :: ";
//        // ✅ Define the required timestamp range (March 2nd - March 10th, 2025) in GMT
//        long startTimestamp = LocalDateTime.of(2025, 3, 2, 18, 0)  // March 2nd, 2025 18:00:00
//                .atZone(ZoneId.of("GMT")) // ✅ Set GMT zone
//                .toInstant().toEpochMilli();
//
//        long endTimestamp = LocalDateTime.of(2025, 3, 10, 23, 59, 59) // March 10th, 2025 23:59:59
//                .atZone(ZoneId.of("GMT")) // ✅ Set GMT zone
//                .toInstant().toEpochMilli();
//        List<ConsumerRecord<String, String>> filteredRecords = records.stream()
//                .filter(rec -> rec.timestamp() >= startTimestamp && rec.timestamp() <= endTimestamp)
//                .collect(Collectors.toList());
//        log.info("--- {} Batch Total MESSAGES {} , Filtered Messages {} ---", batchUUID,records.size(), filteredRecords.size());
//        if (!filteredRecords.isEmpty()) {
//            filteredRecords.forEach(rec -> {
//                messageService.processMessage(rec.value(), batchUUID, rec.key(),rec.timestamp());
//            });
//        }
//        // Manually acknowledge the batch after processing
//        acknowledgment.acknowledge();
//        log.info("--- {} , {} messages got processed for BATCH ---", batchUUID, records.size());
//    }

//    @KafkaListener(id = "gkListener", topics = "${msgs.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
//    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
//        String batchUUID = "BATCH-ID-" + UUID.randomUUID().toString() + " :: ";

//        // ✅ Define the required timestamp range (March 1st - March 6th, 2025) in GMT
//        long startTimestamp = LocalDateTime.of(2025, 3, 4, 0, 0)  // March 1st, 2025 00:00:00
//                .atZone(ZoneId.of("GMT")) // ✅ Set GMT zone
//                .toInstant().toEpochMilli();
//
//        long endTimestamp = LocalDateTime.of(2025, 3, 4, 23, 59, 59) // March 6th, 2025 23:59:59
//                .atZone(ZoneId.of("GMT")) // ✅ Set GMT zone
//                .toInstant().toEpochMilli();

//        List<ConsumerRecord<String, String>> filteredRecords = records.stream()
//                .filter(rec -> rec.timestamp() >= startTimestamp && rec.timestamp() <= endTimestamp)
//                .collect(Collectors.toList());
//        log.info("--- {} Batch Total MESSAGES {} , Filtered Messages {} ---", batchUUID,records.size(), filteredRecords.size());
//        if (!filteredRecords.isEmpty()) {
//            filteredRecords.forEach(rec -> {
//                String msgKey = rec.key();
//                messageService.processMessage(rec.value(), batchUUID, msgKey,rec.timestamp());
//            });
//        }
//
//        // Manually acknowledge the batch after processing
//        acknowledgment.acknowledge();
//        log.info("--- {} , {} messages got processed ---", batchUUID, filteredRecords.size());
//    }

}
