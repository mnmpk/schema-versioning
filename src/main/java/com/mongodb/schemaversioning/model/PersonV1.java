package com.mongodb.schemaversioning.model;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PersonV1 {
    @BsonId
    private ObjectId id;
    private String firstName;
    private String lastName;
    private String address;
    private String city;
    private String state;
    private String telephone;
    private String cellphone;

}