package com.gk.kafka.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "message_collec")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageDocument {
    @Id
    private String id;
    private String tenantId;
    private String partnerMessageId;
    private String messageId;
    private String from;
    private String to;
    private String body;
    private String status;
    private String receivedAt;
    private String processedAt;
    private String dlrReceivedAt;
    private String country;
    private String entityId;
    private String templateId;
    private int messageType;
    private String customId;
    private String metadata;
    private String serviceId;
    private String smsc;
    private int units;
    private int requestCredits;
    private int deliveryCredits;
    private boolean isFlash;
    private String batchId;
    private String parameters;


    private String statusMsg;
}
