package com.mongodb.schemaversioning.controller;

import java.util.ArrayList;
import java.util.List;

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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.PersonV2Relation;

@RestController
@RequestMapping(path = "/api/v2/relation")
public class ApplicationV2RelationController {

    @Autowired
    private MongoTemplate mongoTemplate;

    @GetMapping("/person")
    public List<PersonV2Relation> list() {
        MongoCollection<PersonV2Relation> collection = mongoTemplate.getDb().getCollection("person", PersonV2Relation.class);
        return collection.find().into(new ArrayList<>());
    }

    @GetMapping("/person/{id}")
    public @ResponseBody PersonV2Relation read(@PathVariable String id) {
        MongoCollection<PersonV2Relation> collection = mongoTemplate.getDb().getCollection("person", PersonV2Relation.class);
        return collection.find(Filters.eq("_id", id)).first();
    }

    @PostMapping("/person")
    public InsertOneResult create() {
        MongoCollection<PersonV2Relation> collection = mongoTemplate.getDb().getCollection("person", PersonV2Relation.class);
        List<Contact> contacts = new ArrayList<>();
        contacts.add(Contact.builder().method("cellphone").value("600-900-0003").build());
        contacts.add(Contact.builder().method("telephone").value("400-900-4000").build());
        PersonV2Relation person = PersonV2Relation.builder().id("10001").version("v2").firstName("M").lastName("Ma").address("100 Forest")
                .city("Palo Alto").state("California")/*.telephone("400-900-4000").cellphone("600-900-0003")*/.contacts(contacts).build();
        return collection.insertOne(person);
    }

    @PutMapping("/person/{id}")
    public @ResponseBody UpdateResult update(@PathVariable String id) {
        MongoCollection<PersonV2Relation> collection = mongoTemplate.getDb().getCollection("person", PersonV2Relation.class);
        return collection.updateOne(Filters.and(Filters.eq("_id", id), Filters.eq("contacts.method", "telephone")), Updates.combine(
            Updates.set("version", "v2"),
            Updates.set("contacts", List.of(
                Contact.builder().method("cellphone").value("600-900-0003").build(), 
                Contact.builder().method("telephone").value("800-900-4000").build(), 
                Contact.builder().method("email").value("adams@mongodb.com").build())
            )
        ));
    }

    @DeleteMapping("/person/{id}")
    public DeleteResult delete(@PathVariable String id) {
        MongoCollection<PersonV2Relation> collection = mongoTemplate.getDb().getCollection("person", PersonV2Relation.class);
        return collection.deleteOne(Filters.eq("_id", id));
    }
}
