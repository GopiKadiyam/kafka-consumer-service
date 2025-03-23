package com.gk.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gk.kafka.entities.MessageDocument;
import com.gk.kafka.repository.MessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class MessageService {
    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public void processMessage(String message, String batchUUID, String msgKey) {
        try {
            message = message.replaceAll("[\\r\\n]+", "").trim();
            log.info("##### {}", message);
            JsonNode jsonNode = objectMapper.readTree(message);
            //jsonNode = replaceEmptyObjectsWithNull(jsonNode);
            if (jsonNode.has("Status")) {
                log.info("{} UPDATE MSG operation started for msgKey {}", batchUUID, msgKey);
                handleUpdateMessage(jsonNode, batchUUID, msgKey);
            } else {
                log.info("{} INSERT MSG operation started for msgKey {}", batchUUID, msgKey);
                handleInsertMessage(jsonNode, batchUUID, msgKey);
            }
        } catch (Exception e) {
            // TODO write logic to To Store message into DB
            //e.printStackTrace();
            log.error("#PROCESS_ERR {} getting error while parsing json for msgKey {} ", batchUUID, msgKey);
        }
    }

    private void handleInsertMessage(JsonNode jsonNode, String batchUUID, String kafkaMsgKey) {
        //2025-03-14T13:49:35.184+05:30  INFO 20044 --- [org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1] c.g.k.service.KafkaConsumer              : BATCH-ID-0884bbc9-03a5-4078-a645-dead5f23a218 ConsumerRecord key 52869ec3-629a-41aa-858c-f9e9fb2e0e17 and value {"TenantId":"52869ec3-629a-41aa-858c-f9e9fb2e0e17","ReceivedAt":"2025-02-27T07:40:09.7450638+05:30","Request":{"from":"PORTER","to":"8448820306","country":"IN","body":"<#> Your Porter app verification code is 175460. Thank you. bEsxYhqK5AU - Team Porter","templateId":"1107161139929937618","entityId":"1101693680000010032","messageType":2,"customId":"4bb48fc8-6f50-45e8-a298-3fcac933d3da","metadata":{},"flash":false,"serviceType":0},"BatchId":"68a37061-5bc7-436b-b8cb-10883a8aa7c1","Id":"68a37061-5bc7-436b-b8cb-10883a8aa7c1"} with timestamp 1740622209795
        //2025-03-14T13:49:35.184+05:30  INFO 20044 --- [org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1] c.g.k.service.KafkaConsumer              : BATCH-ID-0884bbc9-03a5-4078-a645-dead5f23a218 ConsumerRecord key 52869ec3-629a-41aa-858c-f9e9fb2e0e17 and value {"Status":"id:S11100572707400873160257197 sub:001 dlvrd:001 submit date:250227074008 done date:250227074008 stat:DELIVRD err:000 text:","Type":"1","PartnerMessageId":"S11100572707400873160257197","Smscid":"TESYNCOTP2","Timestamp":"1740622210","Id":"fbe6d09d-f6d4-4b08-90e8-264dd6cc3755","ServiceId":"673ef5bb45288d4686a298f7","TenantId":"52869ec3-629a-41aa-858c-f9e9fb2e0e17"} with timestamp 1740622210116

        try {
            String messageId = jsonNode.get("Id").asText();
            // Check if document already exists
            Optional<MessageDocument> existingMessageId = messageRepository.findByMessageId(messageId);
            if (existingMessageId.isPresent()) {
                log.info("#DUPINSERT_ERR {} There already existing entry for messageId :: {} in db for kafkaMsgKey {} ", batchUUID, messageId, kafkaMsgKey);
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
                log.info("{} message ID {} inserted into db for kafkaMsgKey", batchUUID, messageId, kafkaMsgKey);
            }
        } catch (Exception e) {
            log.error("#INSERTPARSE_ERR {} unable to process json while INSERT MSG process for kafkaMsgKey {}", batchUUID, kafkaMsgKey);
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

    private void handleUpdateMessage(JsonNode jsonNode, String batchUUID, String kafkaMsgKey) {
        try {
            String messageId = jsonNode.get("Id").asText();
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
//                Query query = new Query(Criteria.where("_id").is(messageDoc.getId()));
//                Update update = new Update()
//                        .set("partnerMessageId", partnerMsgId)
//                        .set("smsc",smsc)
//                        .set("serviceId",serviceId)
//                        .set("processedAt",doneDate)
//                        .set("dlrReceivedAt",doneDate)
//                        .set("status",statusCode);
//
//                mongoTemplate.findAndModify(query, update, MessageDocument.class);
                log.info("BATCH-ID-{} and ID {} updated into db", batchUUID, messageId);
            } else {
                log.info("#UPDATEONNONEXISTINGREC_ERR {}  It's for UPDATE MSG Operation, but ID :: {} is not present in db for kafkaMsgKey {}", batchUUID, messageId, kafkaMsgKey);
                //TODO put entry to error collection
            }
        } catch (Exception e) {
            log.error("#UPDATEPARSE_ERR {} unable to process json for UPDATE MSG process for kafkaMsgKey{}", batchUUID, kafkaMsgKey);
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

//    private static JsonNode replaceEmptyObjectsWithNull(JsonNode node) {
//        if (node.isObject()) {
//            ObjectNode objectNode = (ObjectNode) node;
//            objectNode.fieldNames().forEachRemaining(field -> {
//                JsonNode childNode = objectNode.get(field);
//                if (childNode.isObject() && childNode.size() == 0) { // Check for empty JSON {}
//                    objectNode.set(field, null); // Replace {} with null
//                } else {
//                    replaceEmptyObjectsWithNull(childNode); // Recursive check for nested objects
//                }
//            });
//        }
//        return node;
//    }
}
