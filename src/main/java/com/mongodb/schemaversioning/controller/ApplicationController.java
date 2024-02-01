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
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.schemaversioning.model.PersonV1;

@RestController
@RequestMapping(path = "/api")
public class ApplicationController {

    @Autowired
    private MongoTemplate mongoTemplate;

    @GetMapping("/person/init")
    public InsertManyResult init() {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        collection.deleteMany(Filters.empty());
        List<PersonV1> list = new ArrayList<>();
        for(int i =10000; i<20000;i++){
            list.add(PersonV1.builder().id(String.valueOf(i)).firstName("M").lastName("Ma").address("100 Forest")
            .city("Palo Alto").state("California").telephone("400-900-4000").cellphone("600-900-0003").build());
        }
        return collection.insertMany(list);
    }

    @GetMapping("/person")
    public List<PersonV1> list() {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        return collection.find().into(new ArrayList<>());
    }

    @GetMapping("/person/{id}")
    public @ResponseBody PersonV1 read(@PathVariable String id) {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        return collection.find(Filters.eq("_id", id)).first();
    }

    @PostMapping("/person")
    public InsertOneResult create() {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        PersonV1 person = PersonV1.builder().id("10001").firstName("M").lastName("Ma").address("100 Forest")
                .city("Palo Alto").state("California").telephone("400-900-4000").cellphone("600-900-0003").build();

        return collection.insertOne(person);
    }

    @PutMapping("/person/{id}")
    public @ResponseBody UpdateResult update(@PathVariable String id) {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        return collection.updateOne(Filters.eq("_id", id), Updates.set("telephone", "800-900-4000"));
    }

    @DeleteMapping("/person/{id}")
    public DeleteResult delete(@PathVariable String id) {
        MongoCollection<PersonV1> collection = mongoTemplate.getDb().getCollection("person", PersonV1.class);
        return collection.deleteOne(Filters.eq("_id", id));
    }
}
