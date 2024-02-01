package com.mongodb.schemaversioning.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.Person;
import com.mongodb.schemaversioning.model.PersonV1;
import com.mongodb.schemaversioning.model.PersonV3;
import com.mongodb.schemaversioning.service.SchemaVersionHandlerService;

@RestController
@RequestMapping(path = "/api/v3")
public class ApplicationV3NewCollectionController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    SchemaVersionHandlerService schemaVersionHandlerService;

    @GetMapping("/person/migrate")
    public String migrate() throws InterruptedException {
        MongoCollection<BsonDocument> collection = mongoTemplate.getDb().getCollection("person", BsonDocument.class);
        mongoTemplate.getDb().getCollection("personV3").deleteMany(Filters.empty());
        int threads = 3;
        int batchSize = 10000 / threads;
        MongoCursor<BsonDocument> cursor = collection.find().batchSize(batchSize).cursor();
        List<CompletableFuture<Void>> ends = new ArrayList<CompletableFuture<Void>>();
        List<BsonDocument> subList = new ArrayList<>();
        while (cursor.hasNext()) {
            subList.add(cursor.next());
            if (subList.size() == batchSize) {
                ends.add(schemaVersionHandlerService.batchMigrate(subList));
                subList = new ArrayList<>();
            }
        }
        if (subList.size() > 0) {
            ends.add(schemaVersionHandlerService.batchMigrate(subList));
            subList = new ArrayList<>();
        }
        CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();

        return "OK";
    }

    @GetMapping("/person/start-trigger")
    public String startTrigger() {
        logger.info("start forward trigger");
        CompletableFuture.supplyAsync(() -> {
            try {
                MongoCollection<BsonDocument> collection = mongoTemplate.getDb().getCollection("person",
                        BsonDocument.class);
                collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach((change) -> {
                    switch (change.getOperationType()) {
                        case INSERT:
                        case UPDATE:
                        case REPLACE:
                            var fullDocument = change.getFullDocument();
                            if (!fullDocument.getBoolean("byTrigger").getValue()) {
                                logger.info("v1 is changing:" +
                                        change.getDocumentKey().getString("_id").getValue()
                                        + ", reflect the change to v3");
                                Person p = schemaVersionHandlerService.handleVersionChange(fullDocument);
                                p.setByTrigger(true);
                                mongoTemplate.getDb().getCollection("personV3", PersonV3.class).replaceOne(
                                        Filters.eq("_id", change.getDocumentKey().getString("_id").getValue()),
                                        (PersonV3) p,
                                        new ReplaceOptions().upsert(true));
                            }
                            break;
                        case DELETE:
                            logger.info("v3 is changing:" +
                                    change.getDocumentKey().getString("_id").getValue()
                                    + ", reflect the change to v1");
                            mongoTemplate.getDb().getCollection("personV3", PersonV3.class)
                                    .deleteOne(
                                            Filters.eq("_id", change.getDocumentKey().getString("_id").getValue()));
                            break;
                        default:
                            break;
                    }
                });
            } catch (Exception e) {
                logger.error("forward trigger error", e);
            }
            return null;
        });
        logger.info("start reverse trigger");
        CompletableFuture.supplyAsync(() -> {
            try {
                MongoCollection<BsonDocument> collection = mongoTemplate.getDb().getCollection("personV3",
                        BsonDocument.class);
                collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).forEach((change) -> {
                    switch (change.getOperationType()) {
                        case INSERT:
                        case UPDATE:
                        case REPLACE:
                            var fullDocument = change.getFullDocument();
                            if (!fullDocument.getBoolean("byTrigger").getValue()) {
                                logger.info("v3 is changing:" +
                                        change.getDocumentKey().getString("_id").getValue()
                                        + ", reflect the change to v1");
                                PersonV3 p = (PersonV3) schemaVersionHandlerService.handleVersionChange(fullDocument);
                                PersonV1 pV1 = PersonV1.builder().id(p.getId()).firstName(p.getFirstName())
                                        .lastName(p.getLastName()).address(p.getAddress())
                                        .city(p.getCity()).state(p.getState())
                                        .telephone(p.getContacts().get(0).getValue())
                                        .cellphone(p.getContacts().get(1).getValue()).build();
                                pV1.setByTrigger(true);

                                mongoTemplate.getDb().getCollection("person", PersonV1.class).replaceOne(
                                        Filters.eq("_id", change.getDocumentKey().getString("_id").getValue()), pV1,
                                        new ReplaceOptions().upsert(true));
                            }
                            break;
                        case DELETE:
                            logger.info("v3 is deleting:" +
                                    change.getDocumentKey().getString("_id").getValue()
                                    + ", reflect the change to v1");
                            mongoTemplate.getDb().getCollection("person", PersonV1.class)
                                    .deleteOne(
                                            Filters.eq("_id", change.getDocumentKey().getString("_id").getValue()));
                            break;
                        default:
                            break;
                    }
                });
            } catch (Exception e) {
                logger.error("forward trigger error", e);
            }
            return null;
        });
        return "OK";
    }

    @GetMapping("/person")
    public List<PersonV3> list() {
        MongoCollection<PersonV3> collection = mongoTemplate.getDb().getCollection("personV3", PersonV3.class);
        return collection.find().into(new ArrayList<>());
    }

    @GetMapping("/person/{id}")
    public @ResponseBody PersonV3 read(@PathVariable String id) {
        MongoCollection<PersonV3> collection = mongoTemplate.getDb().getCollection("personV3", PersonV3.class);
        return collection.find(Filters.eq("_id", id)).first();
    }

    @PostMapping("/person")
    public InsertOneResult create() {
        MongoCollection<PersonV3> collection = mongoTemplate.getDb().getCollection("personV3", PersonV3.class);
        List<Contact> contacts = new ArrayList<>();
        contacts.add(Contact.builder().method("cellphone").value("600-900-0003").build());
        contacts.add(Contact.builder().method("telephone").value("400-900-4000").build());
        PersonV3 person = PersonV3.builder().id("10001").version(2).firstName("M").lastName("Ma").address("100 Forest")
                .city("Palo Alto").state("California")
                /* .telephone("400-900-4000").cellphone("600-900-0003") */.contacts(contacts).build();
        return collection.insertOne(person);
    }

    @PutMapping("/person/{id}")
    public @ResponseBody UpdateResult update(@PathVariable String id) {
        MongoCollection<PersonV3> collection = mongoTemplate.getDb().getCollection("personV3", PersonV3.class);
        return collection.updateOne(Filters.and(Filters.eq("_id", id), Filters.eq("contacts.method", "telephone")),
                Updates.combine(
                        Updates.set("version", 2),
                        Updates.set("contacts", List.of(
                                Contact.builder().method("cellphone").value("600-900-0003").build(),
                                Contact.builder().method("telephone").value("800-900-4000").build(),
                                Contact.builder().method("email").value("adams@mongodb.com").build()))));
    }

    @DeleteMapping("/person/{id}")
    public DeleteResult delete(@PathVariable String id) {
        MongoCollection<PersonV3> collection = mongoTemplate.getDb().getCollection("personV3", PersonV3.class);
        return collection.deleteOne(Filters.eq("_id", id));
    }
}
