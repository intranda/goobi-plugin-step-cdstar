package org.goobi.api.mq.ticket;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@XmlRootElement
@JsonIgnoreProperties({"public"})
public class ArchiveInformation {

    private String vault;
    private String id;
    private String revision;


    private String profile;
    private String state;
    private String created;
    private String modified;
    private int file_count = 0;
    private String owner;

    private List<FileInformation> files = new ArrayList<>();

}
