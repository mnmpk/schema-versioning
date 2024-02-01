package com.mongodb.schemaversioning.service;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.mongodb.schemaversioning.model.Contact;
import com.mongodb.schemaversioning.model.Person;
import com.mongodb.schemaversioning.model.PersonV1;
import com.mongodb.schemaversioning.model.PersonV2;
import com.mongodb.schemaversioning.model.PersonV3;

@Service
public class SchemaVersionHandlerService {

    @Autowired
    private CodecRegistry codecRegistry;

    public Person handleVersionChange(BsonDocument doc) {
        int docVersion = 1;
        if (doc.containsKey("version"))
            docVersion = doc.getNumber("version").intValue();
        Person p = convert(docVersion, doc);
        switch (docVersion) {
            case 1:
                p = handleVersion1((PersonV1) p);
            case 2:
                p = handleVersion2((PersonV2) p);
        }
        return p;
    }

    private Person convert(int docVersion, BsonDocument doc) {
        Class<? extends Person> clazz = PersonV1.class;
        switch (docVersion) {
            case 2:
                clazz = PersonV2.class;
                break;
            case 3:
                clazz = PersonV3.class;
                break;
        }
        return codecRegistry.get(clazz).decode(doc.asBsonReader(),
                DecoderContext.builder().build());
    }

    private PersonV2 handleVersion1(PersonV1 v1) {
        System.out.println(v1);
        return new PersonV2(v1.getId(), v1.getFirstName(), v1.getLastName(), v1.getAddress(), v1.getCity(),
                v1.getState(), 2,
                List.of(new Contact("telephone", v1.getTelephone()), new Contact("cellphone", v1.getCellphone())));
    }

    private PersonV3 handleVersion2(PersonV2 v2) {
        System.out.println(v2);
        return new PersonV3(v2.getId(), v2.getFirstName(), v2.getLastName(), v2.getAddress(), v2.getCity(),
                v2.getState(), 3,
                v2.getContacts());
    }
}
