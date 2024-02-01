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
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.schemaversioning.model.PersonV2DeleteField;

@RestController
@RequestMapping(path = "/api/v2/delete-field")
public class ApplicationV2DeleteFieldController {

    @Autowired
    private MongoTemplate mongoTemplate;

    @GetMapping("/person")
    public List<PersonV2DeleteField> list() {
        MongoCollection<PersonV2DeleteField> collection = mongoTemplate.getDb().getCollection("person", PersonV2DeleteField.class);
        return collection.find().into(new ArrayList<>());
    }

    @GetMapping("/person/{id}")
    public @ResponseBody PersonV2DeleteField read(@PathVariable String id) {
        MongoCollection<PersonV2DeleteField> collection = mongoTemplate.getDb().getCollection("person", PersonV2DeleteField.class);
        return collection.find(Filters.eq("_id", id)).first();
    }

    @PostMapping("/person")
    public InsertOneResult create() {
        MongoCollection<PersonV2DeleteField> collection = mongoTemplate.getDb().getCollection("person", PersonV2DeleteField.class);
        PersonV2DeleteField person = PersonV2DeleteField.builder().id("10001").version(2).firstName("M").lastName("Ma").address("100 Forest")
                .city("Palo Alto").state("California")/*.telephone("400-900-4000")*/.build();

        return collection.insertOne(person);
    }

    @PutMapping("/person/{id}")
    public @ResponseBody UpdateResult update(@PathVariable String id) {
        MongoCollection<PersonV2DeleteField> collection = mongoTemplate.getDb().getCollection("person", PersonV2DeleteField.class);
        return collection.updateOne(Filters.eq("_id", id), Updates.combine(
            Updates.set("version", 2)
            //Updates.set("telephone", "800-900-4000")
        ));
    }

    @DeleteMapping("/person/{id}")
    public DeleteResult delete(@PathVariable String id) {
        MongoCollection<PersonV2DeleteField> collection = mongoTemplate.getDb().getCollection("person", PersonV2DeleteField.class);
        return collection.deleteOne(Filters.eq("_id", id));
    }

}
