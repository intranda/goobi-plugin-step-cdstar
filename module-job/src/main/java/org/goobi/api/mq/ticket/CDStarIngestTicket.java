package org.goobi.api.mq.ticket;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketHandler;
import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.PropertyType;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class CDStarIngestTicket implements TicketHandler<PluginReturnValue> {

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        log.info("got ingest ticket for " + ticket.getProcessName());

        String userName = ticket.getProperties().get("userName");
        String password = ticket.getProperties().get("password");
        String url = ticket.getProperties().get("url");
        String vault = ticket.getProperties().get("vault");

        Integer processId = ticket.getProcessId();

        Client client = ClientBuilder.newClient().register(new BasicAuthenticator(userName, password));

        WebTarget goobiBase = client.target(url);
        WebTarget vaultBase = goobiBase.path(vault);

        // create new archive
        ArchiveInformation resp = vaultBase.request(MediaType.APPLICATION_JSON).post(null, ArchiveInformation.class);

        log.info("created archive " + resp.getId());

        // read archive metadata
        WebTarget archiveBase = vaultBase.path(resp.getId());
        Process process = null;
        try {
            process = ProcessManager.getProcessById(processId);
            // save archive id as property
            Processproperty processproperty = new Processproperty();
            processproperty.setProcessId(process.getId());
            processproperty.setProzess(process);
            processproperty.setTitel("archive-id");
            processproperty.setType(PropertyType.GENERAL);
            processproperty.setWert(resp.getId());
            PropertyManager.saveProcessProperty(processproperty);
        } catch (DAOException e) {
            log.error(e);
        }

        // upload master images
        List<Path> masterFiles = null;
        try {
            masterFiles = StorageProvider.getInstance().listFiles(process.getImagesOrigDirectory(false));
        } catch (IOException | SwapException | DAOException e1) {
            log.error(e1);
            return PluginReturnValue.ERROR;
        }

        WebTarget masterTarget = archiveBase.path("" + processId).path("master");

        for (Path p : masterFiles) {
            log.debug("upload file " + p.toString());
            WebTarget imageTarget = masterTarget.path(p.getFileName().toString());

            InputStream imageFile;
            try {
                imageFile = new FileInputStream(p.toFile());

                String mimeType = Files.probeContentType(p);

                imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);

            } catch (IOException e) {
                log.error(e);
                return PluginReturnValue.ERROR;
            }
        }

        // upload derivate images
        List<Path> derivateFiles = null;
        try {
            derivateFiles = StorageProvider.getInstance().listFiles(process.getImagesTifDirectory(false));
        } catch (IOException | SwapException e1) {
            log.error(e1);
            return PluginReturnValue.ERROR;
        }

        WebTarget derivateTarget = archiveBase.path("" + processId).path("derivate");

        for (Path p : derivateFiles) {
            log.debug("upload file " + p.toString());
            WebTarget imageTarget = derivateTarget.path(p.getFileName().toString());

            InputStream imageFile;
            try {
                imageFile = new FileInputStream(p.toFile());

                String mimeType = Files.probeContentType(p);

                imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);

            } catch (IOException e) {
                log.error(e);
                return PluginReturnValue.ERROR;
            }
        }

        List<Path> ocrTxtFiles = null;
        List<Path> ocrAltoFiles = null;
        List<Path> ocrPdfFiles = null;
        try {
            if (StorageProvider.getInstance().isDirectory(Paths.get(process.getOcrTxtDirectory()))) {
                ocrTxtFiles = StorageProvider.getInstance().listFiles(process.getOcrTxtDirectory());
            }
            if (StorageProvider.getInstance().isDirectory(Paths.get(process.getOcrAltoDirectory()))) {
                ocrAltoFiles = StorageProvider.getInstance().listFiles(process.getOcrAltoDirectory());
            }
            if (StorageProvider.getInstance().isDirectory(Paths.get(process.getOcrPdfDirectory()))) {
                ocrPdfFiles = StorageProvider.getInstance().listFiles(process.getOcrPdfDirectory());
            }

        } catch (IOException | SwapException e1) {
            log.error(e1);
            return PluginReturnValue.ERROR;
        }

        WebTarget ocrTarget = archiveBase.path("" + processId).path("ocr");

        if (ocrTxtFiles != null) {
            for (Path p : ocrTxtFiles) {
                log.debug("upload file " + p.toString());
                WebTarget imageTarget = ocrTarget.path(p.getFileName().toString());

                InputStream imageFile;
                try {
                    imageFile = new FileInputStream(p.toFile());

                    String mimeType = Files.probeContentType(p);

                    imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);

                } catch (IOException e) {
                    log.error(e);
                    return PluginReturnValue.ERROR;
                }
            }
        }
        if (ocrAltoFiles != null) {
            for (Path p : ocrAltoFiles) {
                log.debug("upload file " + p.toString());
                WebTarget imageTarget = ocrTarget.path(p.getFileName().toString());

                InputStream imageFile;
                try {
                    imageFile = new FileInputStream(p.toFile());

                    String mimeType = Files.probeContentType(p);

                    imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);

                } catch (IOException e) {
                    log.error(e);
                    return PluginReturnValue.ERROR;
                }
            }
        }

        if (ocrPdfFiles != null) {
            for (Path p : ocrPdfFiles) {
                log.debug("upload file " + p.toString());
                WebTarget imageTarget = ocrTarget.path(p.getFileName().toString());

                InputStream imageFile;
                try {
                    imageFile = new FileInputStream(p.toFile());

                    String mimeType = Files.probeContentType(p);

                    imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);

                } catch (IOException e) {
                    log.error(e);
                    return PluginReturnValue.ERROR;
                }
            }
        }

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
        return "CDStarUpload";
    }

}
