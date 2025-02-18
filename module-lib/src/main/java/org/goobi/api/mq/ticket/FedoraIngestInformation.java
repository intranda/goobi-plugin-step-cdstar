package org.goobi.api.mq.ticket;

import jakarta.xml.bind.annotation.XmlRootElement;

import lombok.Data;

@Data
@XmlRootElement
public class FedoraIngestInformation {

    private String pid;

    private String url;


}
