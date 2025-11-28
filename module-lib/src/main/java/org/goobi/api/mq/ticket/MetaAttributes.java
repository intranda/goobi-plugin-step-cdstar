package org.goobi.api.mq.ticket;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;
import ugh.dl.Corporate;
import ugh.dl.DocStruct;
import ugh.dl.Metadata;
import ugh.dl.Person;

@Data
@XmlRootElement
@JsonIgnoreProperties({ "public" })
public class MetaAttributes {

    @XmlElement(name = "dc:id")
    private List<String> id = new ArrayList<>();

    @XmlElement(name = "dc:type")
    private List<String> type = new ArrayList<>();

    @XmlElement(name = "dc:language")
    private List<String> language = new ArrayList<>();

    @XmlElement(name = "dc:title")
    private List<String> title = new ArrayList<>();

    @XmlElement(name = "dc:subject")
    private List<String> subject = new ArrayList<>();

    @XmlElement(name = "dc:description")
    private List<String> description = new ArrayList<>();

    @XmlElement(name = "dc:creator")
    private List<String> creator = new ArrayList<>();

    @XmlElement(name = "dc:publisher")
    private List<String> publisher = new ArrayList<>();

    @XmlElement(name = "dc:rights")
    private List<String> rights = new ArrayList<>();

    @XmlElement(name = "dc:date")
    private List<String> date = new ArrayList<>();

    @XmlElement(name = "dc:source")
    private List<String> source = new ArrayList<>();

    public void addMetadata(DocStruct docstruct) {
        if (!docstruct.getType().isAnchor()) {
            // TOOD id, source only for volume
            type.add("Text");
            if (docstruct.getAllMetadata() != null) {
                String identifier = null;
                for (Metadata md : docstruct.getAllMetadata()) {
                    if ("CatalogIDDigital".equals(md.getType().getName())) {
                        identifier = md.getValue();
                        break;
                    }
                }
                if (StringUtils.isNotBlank(identifier)) {
                    id.add(identifier);
                    source.add("https://dlc.mpg.de/image/" + identifier + "/1/");
                }
            }

        }

        if (docstruct.getAllMetadata() != null) {
            for (Metadata md : docstruct.getAllMetadata()) {
                switch (md.getType().getName()) {
                    case "DocLanguage":
                        language.add(md.getValue());
                        break;
                    case "TitleDocMain":
                        title.add(md.getValue());
                        break;
                    case "Subject", "SubjectGeographic", "SubjectForm", "SubjectPerson", "SubjectTemporal", "SubjectTitle", "SubjectTopic":
                        subject.add(md.getValue());
                        break;
                    case "ContentDescription":
                        description.add(md.getValue());
                        break;
                    case "AccessLicense":
                        rights.add(md.getValue());
                        break;
                    case "PublicationYear":
                        date.add(md.getValue());
                        break;

                    case "PublisherName":
                        publisher.add(md.getValue());
                        break;
                    default:
                        break;
                }

            }
        }

        if (docstruct.getAllPersons() != null) {
            for (Person p : docstruct.getAllPersons()) {
                switch (p.getType().getName()) {
                    case "Author", "Bearbeiter", "Creator", "Contributor", "Writer":
                        creator.add(p.getLastname() + ", " + p.getFirstname());
                        break;
                    case "Editor", "PublisherPerson":
                        publisher.add(p.getLastname() + ", " + p.getFirstname());
                        break;
                    default:
                        break;
                }
            }
        }

        if (docstruct.getAllCorporates() != null) {
            for (Corporate c : docstruct.getAllCorporates()) {
                switch (c.getType().getName()) {
                    case "CorporateAuthor", "CorporateContributor", "WriterCorporate":
                        creator.add(c.getMainName());
                        break;
                    case "IssuingBody":
                        publisher.add(c.getMainName());
                        break;
                    default:
                        break;
                }
            }
        }
    }

}
