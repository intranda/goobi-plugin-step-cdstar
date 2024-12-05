package org.goobi.api.mq.ticket;

import java.io.File;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketHandler;
import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.enums.PropertyType;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FedoraIngestTicket implements TicketHandler<PluginReturnValue> {

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        log.info("got fedora ticket for " + ticket.getProcessName());

        String fedoraUrl = ticket.getProperties().get("fedoraUrl");

        String metsfile = ticket.getProperties().get("metsfile");

        log.info("mets file to upload is " + metsfile);

        Integer processId = ticket.getProcessId();

        Client client = ClientBuilder.newClient();
        client.register(MultiPartFeature.class);

        client.register(MultiPartFeature.class);
        FedoraIngestInformation resp = null;
        Process process = null;
        try {
            process = ProcessManager.getProcessById(processId);
            FormDataMultiPart multiPart = new FormDataMultiPart();
            FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("resource", new File(metsfile), MediaType.TEXT_XML_TYPE);
            multiPart.bodyPart(fileDataBodyPart);
            WebTarget fedoraBase = client.target(fedoraUrl);
            resp = fedoraBase.request("application/json;charset=UTF-8")
                    .post(Entity.entity(multiPart, multiPart.getMediaType()),
                            FedoraIngestInformation.class);
        } catch (Exception e) {
            log.error(e);
            return PluginReturnValue.ERROR;

        }

        log.debug("uploaded to " + resp.getUrl());

        // save pid and url as properties

        Processproperty pidProperty = new Processproperty();
        pidProperty.setProcessId(process.getId());
        pidProperty.setProzess(process);
        pidProperty.setTitel("fedora-pid");
        pidProperty.setType(PropertyType.GENERAL);
        pidProperty.setWert(resp.getPid());

        Processproperty urlProperty = new Processproperty();
        urlProperty.setProcessId(process.getId());
        urlProperty.setProzess(process);
        urlProperty.setTitel("fedora-url");
        urlProperty.setType(PropertyType.GENERAL);
        urlProperty.setWert(resp.getUrl());
        try {
            PropertyManager.saveProcessProperty(urlProperty);
            PropertyManager.saveProcessProperty(pidProperty);
        } catch (Exception e) {
            log.error(e);
        }

        log.debug("saved properties");

        // close step
        String closeStepValue = ticket.getProperties().get("closeStep");

        if (StringUtils.isNotBlank(closeStepValue) && "true".equals(closeStepValue)) {
            Step stepToClose = null;

            for (Step processStep : process.getSchritte()) {
                if (processStep.getTitel().equals(ticket.getStepName())) {
                    stepToClose = processStep;
                    break;
                }
            }
            if (stepToClose != null) {
                CloseStepHelper.closeStep(stepToClose, null);
            }
        }
        return PluginReturnValue.FINISH;
    }

    @Override
    public String getTicketHandlerName() {
        return "FedoraIngest";
    }

    public static void main(String[] args) {
        String fedoraUrl = "https://c104-087.cloud.gwdg.de/api/containers/";
        String metsfile = "/opt/digiverso/viewer/hotfolder/808840800.xml";
        Client client = ClientBuilder.newClient();
        client.register(MultiPartFeature.class);

        FormDataMultiPart multiPart = new FormDataMultiPart();
        FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("resource", new File(metsfile), MediaType.TEXT_XML_TYPE);
        multiPart.bodyPart(fileDataBodyPart);
        WebTarget fedoraBase = client.target(fedoraUrl);
        //        Response response = fedoraBase.request("application/json;charset=UTF-8").post(Entity.entity(multiPart, multiPart.getMediaType()));
        FedoraIngestInformation resp = fedoraBase.request("application/json;charset=UTF-8")
                .post(Entity.entity(multiPart, multiPart.getMediaType()),
                        FedoraIngestInformation.class);
        System.out.println(resp.getPid());
        System.out.println(resp.getUrl());

    }
}
