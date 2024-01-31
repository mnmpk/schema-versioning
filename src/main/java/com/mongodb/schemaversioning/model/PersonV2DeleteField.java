package com.mongodb.schemaversioning.model;

import org.bson.codecs.pojo.annotations.BsonId;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PersonV2DeleteField {
    @BsonId
    private String id;
    private String firstName;
    private String lastName;
    private String address;
    private String city;
    private String state;
    private String version;
    private String cellphone;

}