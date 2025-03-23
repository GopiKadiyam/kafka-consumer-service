package com.gk.kafka.repository;

import com.gk.kafka.entities.MessageDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MessageRepository extends MongoRepository<MessageDocument, String> {
    Optional<MessageDocument> findByMessageId(String messageId);
}

