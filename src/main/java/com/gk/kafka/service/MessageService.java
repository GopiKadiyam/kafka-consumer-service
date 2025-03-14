package com.gk.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gk.kafka.entities.MessageDocument;
import com.gk.kafka.repository.MessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class MessageService {
    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private ObjectMapper objectMapper;

    public void processMessage(String message, String batchUUID,String msgKey) {
        try {
            message = message.replaceAll("[\\r\\n]+", "").trim();
            JsonNode jsonNode = objectMapper.readTree(message);
            jsonNode = replaceEmptyObjectsWithNull(jsonNode);
            if (jsonNode.has("Status")) {
                log.info("{} UPDATE MSG operation started for msgKey {}", batchUUID,msgKey);
                handleUpdateMessage(jsonNode, message, batchUUID,msgKey);
            } else {
                log.info("{} INSERT MSG operation started for msgKey {}", batchUUID,msgKey);
                handleInsertMessage(jsonNode, message, batchUUID,msgKey);
            }
        } catch (Exception e) {
            // TODO write logic to To Store message into DB
            //e.printStackTrace();
            log.error("ERROR_RETRY {} getting error while parsing json for msgKey {} ", batchUUID,msgKey);
        }
    }

    private void handleInsertMessage(JsonNode jsonNode, String message, String batchUUID,String kafkaMsgKey) {
        try {
            String id = jsonNode.get("Id").asText();
            // Check if document already exists
            Optional<MessageDocument> existingMessage = messageRepository.findById(id);
            existingMessage.ifPresent(md -> {
                log.info("{} message id :: {} is already message present in db for kafkaMsgKey {}", batchUUID, id,kafkaMsgKey);
            });
            if (existingMessage.isEmpty()) {
                MessageDocument messageDoc = new MessageDocument();
                messageDoc.setId(id);
                messageDoc.setTenantId(jsonNode.get("TenantId").asText());
                messageDoc.setReceivedAt(jsonNode.get("ReceivedAt").asText());

                JsonNode request = jsonNode.get("Request");
                messageDoc.setFrom(request.get("from").asText());
                messageDoc.setTo(request.get("to").asText());
                messageDoc.setCountry(request.get("country").asText());
                messageDoc.setBody(request.get("body").asText());
                messageDoc.setTemplateId(request.get("templateId").asText());
                messageDoc.setEntityId(request.get("entityId").asText());
                messageDoc.setMessageType(request.get("messageType").asInt());
                messageDoc.setCustomId(request.get("customId").asText());
                messageDoc.setMetadata(request.get("metadata").asText());
                messageDoc.setFlash(request.get("flash").asBoolean());
                //TODO Need to check map for "serviceType": 0

                messageDoc.setBatchId(jsonNode.get("BatchId").asText());

                messageRepository.save(messageDoc);
                log.info("{} message ID {} inserted into db for kafkaMsgKey", batchUUID, id,kafkaMsgKey);
            }
        } catch (Exception e) {
            log.error("ERROR_RETRY {} unable to process json while INSERT MSG process for kafkaMsgKey {}", batchUUID, kafkaMsgKey);
        }
    }

    private void handleUpdateMessage(JsonNode jsonNode, String message, String batchUUID,String kafkaMsgKey) {
        try {
            String id = jsonNode.get("Id").asText();
            Optional<MessageDocument> existingMessage = messageRepository.findById(id);

            if (existingMessage.isPresent()) {
                MessageDocument messageDoc = existingMessage.get();
                String status = jsonNode.get("Status").asText();
                messageDoc.setStatusMsg(status);
                messageDoc.setPartnerMessageId(jsonNode.get("PartnerMessageId").asText());
                messageDoc.setSmsc(jsonNode.get("Smscid").asText());
                messageDoc.setServiceId(jsonNode.get("ServiceId").asText());

                String doneDateStr = extractFieldFromSource(status, "done date:(\\d{12})");
                if (doneDateStr != null) {
                    messageDoc.setDlrReceivedAt(convertToIsoFormat(doneDateStr));
                }
                messageDoc.setStatus(extractFieldFromSource(status, "stat:([^\\s]+)"));
                messageRepository.save(messageDoc);
                log.info("BATCH-ID-{} and ID {} updated into db", batchUUID, id);
            } else {
                log.info("ERROR_RETRY {}  It's for UPDATE MSG Operation, but ID :: {} is not present in db for kafkaMsgKey {}", batchUUID, id,kafkaMsgKey);
                //TODO put entry to error collection
            }
        } catch (Exception e) {
            log.error("ERROR_RETRY {} unable to process json for UPDATE MSG process for kafkaMsgKey{}", batchUUID,kafkaMsgKey);
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

    private String convertToIsoFormat(String yymmddHHmmSS) {
        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyMMddHHmmss");
            LocalDateTime localDateTime = LocalDateTime.parse(yymmddHHmmSS, inputFormatter);
            return localDateTime.atZone(ZoneId.of("Asia/Kolkata")).toString();
        } catch (Exception e) {
            log.info("Error while converting doneDate from format yyMMddHHmmss to other format");
            return null;
        }
    }

    private static JsonNode replaceEmptyObjectsWithNull(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.fieldNames().forEachRemaining(field -> {
                JsonNode childNode = objectNode.get(field);
                if (childNode.isObject() && childNode.size() == 0) { // Check for empty JSON {}
                    objectNode.set(field, null); // Replace {} with null
                } else {
                    replaceEmptyObjectsWithNull(childNode); // Recursive check for nested objects
                }
            });
        }
        return node;
    }
}
