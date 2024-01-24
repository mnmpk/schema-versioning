package com.mongodb.schemaversioning.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Contact {
    private String method;
    private String value;

}
