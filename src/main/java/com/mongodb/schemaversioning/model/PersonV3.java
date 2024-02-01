package com.mongodb.schemaversioning.model;

import java.util.List;

import org.bson.codecs.pojo.annotations.BsonId;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PersonV3 extends Person {
    @BsonId
    private String id;
    private String firstName;
    private String lastName;
    private String address;
    private String city;
    private String state;
    private int version;
    private List<Contact> contacts;

}