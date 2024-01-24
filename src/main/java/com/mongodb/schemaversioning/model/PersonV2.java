package com.mongodb.schemaversioning.model;

import java.util.List;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PersonV2 {
    @BsonId
    private ObjectId id;
    private String firstName;
    private String lastName;
    private String address;
    private String city;
    private String state;
    private String schemaVersion;
    private List<Contact> contacts;

}