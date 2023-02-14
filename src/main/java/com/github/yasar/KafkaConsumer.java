package com.github.yasar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private WikiMediaRepository repository;

    public KafkaConsumer(WikiMediaRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "wikimedia_topic2" , groupId = "wikimedia_group")
    public void consume(String eventMessage){

        log.info(String.format("#Event Message Received: %s", eventMessage));
        WikiMedia wikiMedia = new WikiMedia();
        wikiMedia.setWikiEventData(eventMessage);

        repository.save(wikiMedia);

    }

}
