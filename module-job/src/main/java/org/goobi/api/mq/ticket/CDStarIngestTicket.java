package org.goobi.api.mq.ticket;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketHandler;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.export.download.ExportMets;
import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.ExportFileException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.helper.exceptions.UghHelperException;
import de.sub.goobi.persistence.managers.ProcessManager;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.log4j.Log4j2;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.Prefs;
import ugh.exceptions.DocStructHasNoTypeException;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.UGHException;
import ugh.exceptions.WriteException;

@Log4j2
public class CDStarIngestTicket implements TicketHandler<PluginReturnValue> {

    // API documentation: https://cdstar.gwdg.de/docs/dev/

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        log.info("got ingest ticket for " + ticket.getProcessName());

        String userName = ticket.getProperties().get("userName");
        String password = ticket.getProperties().get("password");
        String url = ticket.getProperties().get("url");
        String vault = ticket.getProperties().get("vault");

        Integer processId = ticket.getProcessId();

        Client client = ClientBuilder.newClient()
                .register(new ClientRequestFilter() {
                    @Override
                    public void filter(ClientRequestContext ctx) {
                        String uri = ctx.getUri().toString();
                        // replace queryparam ?param= with ?param
                        uri = uri.replaceAll("=(&|$)", "$1");
                        ctx.setUri(java.net.URI.create(uri));
                    }
                })
                .register(new BasicAuthenticator(userName, password));

        WebTarget goobiBase = client.target(url);
        WebTarget vaultBase = goobiBase.path(vault);

        // check if property/metadata for archive-id already exists
        // if yes: re-use, update existing ingest
        // if not: new archive, store id as property/metadata
        Process process = null;
        try {
            process = ProcessManager.getProcessById(processId);
        } catch (Exception e) {
            log.error(e);
        }
        if (process == null) {
            // this can only happen, if the process gets deleted while in queue
            return PluginReturnValue.ERROR;
        }

        // read metadata file
        DocStruct anchor = null;
        DocStruct logical = null;
        Fileformat fileformat = null;
        try {
            fileformat = process.readMetadataFile();
            logical = fileformat.getDigitalDocument().getLogicalDocStruct();
            if (logical.getType().isAnchor()) {
                anchor = logical;
                logical = logical.getAllChildren().get(0);
            }
        } catch (UGHException | IOException | SwapException e) {
            log.error(e);
            return PluginReturnValue.ERROR;
        }

        Metadata cdstarId = null;

        if (logical.getAllMetadata() != null) {
            for (Metadata md : logical.getAllMetadata()) {
                if ("CDStarID".equals(md.getType().getName())) {
                    cdstarId = md;
                    break;
                }
            }
        }

        // save archive id as property
        // TODO #27304 save as metadata value instead

        //        GoobiProperty processproperty = null;
        //        for (GoobiProperty prop : process.getEigenschaften()) {
        //            if ("archive-id".equals(prop.getPropertyName())) {
        //                processproperty = prop;
        //                break;
        //            }
        //        }
        WebTarget archiveBase = null;

        boolean archiveExists = false;

        //        if (processproperty != null) {
        //            // check if archive id is still valid
        //            Response resp = vaultBase.path(processproperty.getPropertyValue()).request().get();
        //            int statusCode = resp.getStatus();
        //            // 20x, 30x are valid
        //            archiveExists = statusCode < 400;
        //        } else {
        //            // create property
        //            processproperty = new GoobiProperty(PropertyOwnerType.PROCESS);
        //            processproperty.setObjectId(process.getId());
        //            processproperty.setOwner(process);
        //            processproperty.setPropertyName("archive-id");
        //            processproperty.setType(PropertyType.GENERAL);
        //        }

        if (cdstarId != null) {
            // check if archive id is still valid
            Response resp = vaultBase.path(cdstarId.getValue()).request().get();
            int statusCode = resp.getStatus();
            // 20x, 30x are valid
            archiveExists = statusCode < 400;
        } else {
            // create metadata
            Prefs prefs = process.getRegelsatz().getPreferences();
            try {
                cdstarId = new Metadata(prefs.getMetadataTypeByName("CDStarID"));
                logical.addMetadata(cdstarId);
            } catch (MetadataTypeNotAllowedException e) {
                log.error(e);
            }
        }

        if (!archiveExists) {
            // first ingest

            // create new archive
            ArchiveInformation resp = vaultBase.request(MediaType.APPLICATION_JSON).post(null, ArchiveInformation.class);
            log.info("created archive " + resp.getId());

            // read archive metadata
            archiveBase = vaultBase.path(resp.getId());

            //store id
            cdstarId.setValue(resp.getId());

            try {
                process.writeMetadataFile(fileformat);
            } catch (WriteException | PreferencesException | IOException | SwapException e) {
                log.error(e);
            }
        } else {
            // update existing ingest
            archiveBase = vaultBase.path(cdstarId.getValue());
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

        try {
            uploadFile(masterFiles, masterTarget);
        } catch (IOException e) {
            log.error(e);
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

        try {
            uploadFile(derivateFiles, derivateTarget);
        } catch (IOException e) {
            log.error(e);
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

        try {
            if (ocrTxtFiles != null) {
                uploadFile(ocrTxtFiles, ocrTarget);
            }
            if (ocrAltoFiles != null) {
                uploadFile(ocrAltoFiles, ocrTarget);
            }

            if (ocrPdfFiles != null) {
                uploadFile(ocrPdfFiles, ocrTarget);
            }
        } catch (IOException e) {
            log.error(e);
        }

        // #27305 add external mets file to cdstar archive
        try {
            createMetsFile(ticket, process, archiveBase);
        } catch (IOException e) {
            log.error(e);
            return PluginReturnValue.ERROR;
        }

        // #27306 add metadata to archive
        MetaAttributes attrs = new MetaAttributes();
        attrs.addMetadata(logical);
        if (anchor != null) {
            attrs.addMetadata(anchor);
        }

        Response resp = archiveBase.queryParam("meta", "")
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.entity(attrs, MediaType.APPLICATION_JSON));
        if (resp.getStatus() < 400) {

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
        } else {
            // metadata ingest/update was not successful
            return PluginReturnValue.ERROR;
        }

    }

    public void createMetsFile(TaskTicket ticket, Process process, WebTarget archiveBase) throws IOException {
        // export mets file(s) into temporary folder
        Path metsFile = Paths.get(ConfigurationHelper.getInstance().getTemporaryFolder(), process.getTitel() + "_mets.xml");
        try {
            ExportMets exp = new ExportMets();
            exp.startExport(process, metsFile.getParent().toString() + "/");
        } catch (UGHException | DocStructHasNoTypeException | InterruptedException | ExportFileException | UghHelperException
                | SwapException | DAOException e) {
            log.error(e);
        }
        // ingest mets file
        WebTarget metsTarget = archiveBase.path("" + ticket.getProcessId()).path("mets").path("mets.xml");
        try (InputStream inputStream = new FileInputStream(metsFile.toFile())) {
            metsTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(inputStream, "application/xml"), FileInformation.class);
        }

        // ingest anchor file
        Path anchorFile = Paths.get(metsFile.getParent().toString(), metsFile.getFileName().toString().replace(".xml", "_anchor.xml"));
        if (StorageProvider.getInstance().isFileExists(anchorFile)) {
            WebTarget anchorTarget = archiveBase.path("" + ticket.getProcessId()).path("mets").path("anchor.xml");
            try (InputStream inputStream = new FileInputStream(anchorFile.toFile())) {
                anchorTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(inputStream, "application/xml"), FileInformation.class);
            }
        }
        // remove temp files
        if (StorageProvider.getInstance().isFileExists(anchorFile)) {
            StorageProvider.getInstance().deleteFile(anchorFile);
        }
        if (StorageProvider.getInstance().isFileExists(metsFile)) {
            StorageProvider.getInstance().deleteFile(metsFile);
        }
    }

    public void uploadFile(List<Path> files, WebTarget targetBase) throws IOException {
        for (Path p : files) {
            log.debug("upload file " + p.toString());
            WebTarget imageTarget = targetBase.path(p.getFileName().toString());

            try (InputStream imageFile = new FileInputStream(p.toFile())) {
                String mimeType = Files.probeContentType(p);
                imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType), FileInformation.class);
            }

        }
    }

    @Override
    public String getTicketHandlerName() {
        return "CDStarUpload";
    }

}
