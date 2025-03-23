package com.gk.kafka.config;

import com.gk.kafka.entities.MessageDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.stereotype.Component;

@Component
public class MessageDocumentListener extends AbstractMongoEventListener<MessageDocument> {

    @Override
    public void onBeforeSave(BeforeSaveEvent<MessageDocument> event) {
        Document document = event.getDocument();
        if (document != null && document.get("_id") instanceof ObjectId) {
            document.put("_id", document.get("_id").toString());  // Convert ObjectId to String before saving
        }
        super.onBeforeSave(event);
    }
}
