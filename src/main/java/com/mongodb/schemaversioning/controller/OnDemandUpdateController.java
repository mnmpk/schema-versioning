package com.mongodb.schemaversioning.controller;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonDocument;
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.Person;
import com.mongodb.schemaversioning.model.PersonV2;
import com.mongodb.schemaversioning.service.SchemaVersionHandlerService;

@RestController
@RequestMapping(path = "/api/v2")
public class OnDemandUpdateController {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    SchemaVersionHandlerService schemaVersionHandlerService;

    @GetMapping("/person")
    public List<Person> list() {
        MongoCollection<BsonDocument> collection = mongoTemplate.getDb().getCollection("person", BsonDocument.class);
        return collection.find().map(doc->{
            Person p= schemaVersionHandlerService.handleVersionChange(doc);
            mongoTemplate.getDb().getCollection("person", Person.class).replaceOne(Filters.eq("_id", doc.getString("_id")), p);
            return p;
        }).into(new ArrayList<>());
    }

    @GetMapping("/person/{id}")
    public @ResponseBody Person read(@PathVariable String id) {
        MongoCollection<BsonDocument> collection = mongoTemplate.getDb().getCollection("person", BsonDocument.class);
        BsonDocument doc = collection.find(Filters.eq("_id", id)).first();
        Person p = schemaVersionHandlerService.handleVersionChange(doc);
        mongoTemplate.getDb().getCollection("person", Person.class).replaceOne(Filters.eq("_id", doc.getString("_id")), p);
        return p;
    }

    @PostMapping("/person")
    public InsertOneResult create() {
        MongoCollection<PersonV2> collection = mongoTemplate.getDb().getCollection("person", PersonV2.class);
        List<Contact> contacts = new ArrayList<>();
        contacts.add(Contact.builder().method("cellphone").value("600-900-0003").build());
        contacts.add(Contact.builder().method("telephone").value("400-900-4000").build());
        PersonV2 person = PersonV2.builder().id("10001").version(2).firstName("M").lastName("Ma").address("100 Forest")
                .city("Palo Alto").state("California")/*.telephone("400-900-4000").cellphone("600-900-0003")*/.contacts(contacts).build();
        return collection.insertOne(person);
    }

    @PutMapping("/person/{id}")
    public @ResponseBody UpdateResult update(@PathVariable String id) {
        MongoCollection<PersonV2> collection = mongoTemplate.getDb().getCollection("person", PersonV2.class);
        return collection.updateOne(Filters.and(Filters.eq("_id", id), Filters.eq("contacts.method", "telephone")), Updates.combine(
            Updates.set("version", 2),
            Updates.set("contacts", List.of(
                Contact.builder().method("cellphone").value("600-900-0003").build(), 
                Contact.builder().method("telephone").value("800-900-4000").build(), 
                Contact.builder().method("email").value("adams@mongodb.com").build())
            )
        ));
    }

    @DeleteMapping("/person/{id}")
    public DeleteResult delete(@PathVariable String id) {
        MongoCollection<PersonV2> collection = mongoTemplate.getDb().getCollection("person", PersonV2.class);
        return collection.deleteOne(Filters.eq("_id", id));
    }
}
