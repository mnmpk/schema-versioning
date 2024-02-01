package com.mongodb.schemaversioning.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.BsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.Person;
import com.mongodb.schemaversioning.model.PersonV1;
import com.mongodb.schemaversioning.model.PersonV2;
import com.mongodb.schemaversioning.model.PersonV3;

@Service
public class SchemaVersionHandlerService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private CodecRegistry codecRegistry;

    public Person handleVersionChange(BsonDocument doc) {
        int docVersion = 1;
        if (doc.containsKey("version"))
            docVersion = doc.getNumber("version").intValue();
        Person p = convert(docVersion, doc);
        switch (docVersion) {
            case 1:
                p = handleVersion1((PersonV1) p);
            case 2:
                p = handleVersion2((PersonV2) p);
        }
        return p;
    }

    private Person convert(int docVersion, BsonDocument doc) {
        Class<? extends Person> clazz = PersonV1.class;
        switch (docVersion) {
            case 2:
                clazz = PersonV2.class;
                break;
            case 3:
                clazz = PersonV3.class;
                break;
        }
        return codecRegistry.get(clazz).decode(doc.asBsonReader(),
                DecoderContext.builder().build());
    }

    private PersonV2 handleVersion1(PersonV1 v1) {
        return new PersonV2(v1.getId(), v1.getFirstName(), v1.getLastName(), v1.getAddress(), v1.getCity(),
                v1.getState(), 2,
                List.of(new Contact("telephone", v1.getTelephone()), new Contact("cellphone", v1.getCellphone())));
    }

    private PersonV3 handleVersion2(PersonV2 v2) {
        return new PersonV3(v2.getId(), v2.getFirstName(), v2.getLastName(), v2.getAddress(), v2.getCity(),
                v2.getState(), 3,
                v2.getContacts());
    }

    public PersonV1 handleVersion3Fallback(PersonV3 p) {
        return PersonV1.builder().id(p.getId()).firstName(p.getFirstName())
                .lastName(p.getLastName()).address(p.getAddress())
                .city(p.getCity()).state(p.getState())
                .cellphone(p.getContacts().get(0).getValue())
                .telephone(p.getContacts().get(1).getValue()).build();
    }

    @Async
    public CompletableFuture<Void> batchConvert(List<BsonDocument> list) throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        for (BsonDocument doc : list) {
            logger.info("Upgrading " + doc.getString("_id"));
            mongoTemplate.getCollection("person").withDocumentClass(Person.class)
                    .replaceOne(Filters.eq("_id", doc.getString("_id")), handleVersionChange(doc));
            Thread.sleep(500);
        }
        logger.info(Thread.currentThread().getName() + " Complete batch schma upgrade at: "
                + LocalDateTime.now().toString());
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> batchMigrate(List<BsonDocument> list) throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        List<WriteModel<Person>> bulkOperations = new ArrayList<WriteModel<Person>>();
        for (BsonDocument doc : list) {
            bulkOperations
                    .add(new ReplaceOneModel<>(Filters.eq("_id", doc.getString("_id")), handleVersionChange(doc),
                            new ReplaceOptions().upsert(true)));
        }
        mongoTemplate.getCollection("personV3").withDocumentClass(Person.class).bulkWrite(bulkOperations);
        logger.info(Thread.currentThread().getName() + " Complete batch schma migrate at: "
                + LocalDateTime.now().toString());
        return CompletableFuture.completedFuture(null);
    }
}
