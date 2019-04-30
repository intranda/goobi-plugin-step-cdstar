package org.goobi.api.mq.ticket;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class FileInformation {

    private String id;
    private String name;
    private String type;
    private int size;
    private String created;
    private String modified;

    private Map<String, String> digests = new HashMap<String, String>();

}
