package com.mongodb.schemaversioning.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.PersonV1;
import com.mongodb.schemaversioning.model.PersonV2;

@RestController
public class ApplicationController {

    @Autowired
    private MongoTemplate mongoTemplate;



    @GetMapping("/api/v1/insert-person")
    public InsertOneResult insertPersonV1() {
        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<PersonV1> collection = database.getCollection("person", PersonV1.class);
        return collection.insertOne(PersonV1.builder().firstName("M").lastName("Ma").address("100 Forest").city("Palo Alto").state("California").telephone("400-900-4000").cellphone("600-900-0003").build());
    }
    @GetMapping("/api/v2/insert-person")
    public InsertOneResult insertPersonV2() {
        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<PersonV2> collection = database.getCollection("person", PersonV2.class);
        List<Contact> contacts = new ArrayList<>();
        contacts.add(Contact.builder().method("cellphone").value("600-900-0003").build());
        contacts.add(Contact.builder().method("telephone").value("400-900-4000").build());
        PersonV2 person = PersonV2.builder().firstName("M").lastName("Ma").address("100 Forest").city("Palo Alto").state("California").contacts(contacts).build();
        return collection.insertOne(person);
    }

    
}
