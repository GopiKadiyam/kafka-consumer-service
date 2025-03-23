package com.gk.kafka.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.Map;

@Document(collection = "rep_sms_interactions")
@JsonInclude(JsonInclude.Include.NON_NULL)
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class MessageDocument {
    @Id
    @Field("_id")
    private String id;
    private String tenantId;
    private String partnerMessageId;
    private String messageId;
    private String from;
    private String to;
    private String body;
    private String status;
    private Date receivedAt;  // TODO type {$date : "2025-03-06T16:30:07.814Z"}
    private Date processedAt; // TODO type {$date : "2025-03-06T16:30:07.814Z"}
    private Date dlrReceivedAt;   //TODO type {$date : "2025-03-06T16:30:07.814Z"}
    private String country;
    private String entityId;
    private String templateId;
    private int messageType;
    private String customId;
    private Map<String, Object> metadata; //TODO Type {} ,Object
    private String serviceId;
    private String smsc;

    private int units;   // if body length <=160  then 1 unit , ele divide by 153 then round off value.
                        // if body contains rather than ASCII then upto <=70 1 unit, else divide by 67 then round off
                        // Note : ignore \n
    private int requestCredits;
    private int deliveryCredits; //
    private boolean isFlash;
    private String batchId;
    private Map<String, Object> parameters;  //TODO Type {} ,Object
}
