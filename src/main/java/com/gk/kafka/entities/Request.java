package com.gk.kafka.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Request {
    public String from;
    public String to;
    public String country;
    public String body;
    public String templateId;
    public String entityId;
    public int messageType;
    public String customId;
    public Object metadata;
    public boolean flash;
    public int serviceType;
}
