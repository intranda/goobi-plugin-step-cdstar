package org.goobi.api.mq.ticket;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketHandler;
import org.goobi.beans.Process;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.extern.log4j.Log4j;

@Log4j
public class CDStarIngestTicket implements TicketHandler<PluginReturnValue> {

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        String userName = ticket.getProperties().get("userName");
        String password = ticket.getProperties().get("password");
        String url = ticket.getProperties().get("url");
        String vault = ticket.getProperties().get("vault");

        Integer processId = ticket.getProcessId();

        Process process = ProcessManager.getProcessById(processId);

        Client client = ClientBuilder.newClient().register(new BasicAuthenticator(userName, password));

        WebTarget goobiBase = client.target(url);
        WebTarget vaultBase = goobiBase.path(vault);

        // create new archive
        ArchiveInformation resp = vaultBase.request(MediaType.APPLICATION_JSON).post(null, ArchiveInformation.class);

        // read archive metadata
        WebTarget archiveBase = vaultBase.path(resp.getId());

        //        ArchiveInformation archiveInfo = archiveBase.request(MediaType.APPLICATION_JSON).get(ArchiveInformation.class);

        //        System.out.println(archiveInfo.getProfile());
        //        System.out.println(archiveInfo.getState());
        //        System.out.println(archiveInfo.getCreated());
        //        System.out.println(archiveInfo.getModified());
        //        System.out.println(archiveInfo.getFile_count());
        //        System.out.println(archiveInfo.getOwner());

        // upload metadata file

        //        InputStream metadataFile = new FileInputStream("/opt/digiverso/goobi/metadata/1165/meta.xml");


        //        String fileName = "meta.xml";
        //        WebTarget metaFile = archiveBase.path(folderName).path(fileName);
        //        FileInformation fi = metaFile.request(MediaType.APPLICATION_JSON).put(Entity.entity(metadataFile, "application/xml"), FileInformation.class);
        //
        //        System.out.println(fi.getId());
        //        System.out.println(fi.getName());
        //
        //        String url = metaFile.getUri().toASCIIString();
        //        System.out.println(url);

        // upload images
        List<Path> imageFiles = null;
        try {
            imageFiles = StorageProvider.getInstance().listFiles(process.getImagesOrigDirectory(false));
        } catch (IOException | InterruptedException | SwapException | DAOException e1) {
            log.error(e1);
            return PluginReturnValue.ERROR;
        }

        WebTarget masterTarget = archiveBase.path("" + processId).path("master");

        for (Path p : imageFiles) {
            WebTarget imageTarget = masterTarget.path(p.getFileName().toString());

            InputStream imageFile;
            try {
                imageFile = new FileInputStream(p.toFile());
                // jpeg or tiff?

                String mimeType = Files.probeContentType(p);

                FileInformation imageFileInformation = imageTarget.request(MediaType.APPLICATION_JSON).put(Entity.entity(imageFile, mimeType),
                        FileInformation.class);

                //                System.out.println(imageTarget.getUri().toASCIIString());
                //                System.out.println(imageFileInformation.getName());
            } catch (IOException e) {
                log.error(e);
            }

        }

        // finally get back all file names + urls
        //        http://127.0.0.1:9090/v3/demo/9bd872c32e9a01ff?with=files
        ArchiveInformation data = archiveBase.queryParam("with", "files").request(MediaType.APPLICATION_JSON).get(ArchiveInformation.class);
        System.out.println("***********************");
        System.out.println(data.getProfile());
        System.out.println(data.getState());
        System.out.println(data.getCreated());
        System.out.println(data.getModified());
        System.out.println(data.getFile_count());
        System.out.println(data.getOwner());

        for (FileInformation info : data.getFiles()) {
            System.out.println("*******");
            System.out.println(info.getId());
            System.out.println(info.getName());
            System.out.println(info.getType());
            System.out.println(info.getSize());
            System.out.println(info.getCreated());
            System.out.println(info.getModified());
        }

        // TODO run ExportTicket

        return PluginReturnValue.FINISH;
    }

    @Override
    public String getTicketHandlerName() {
        return "CDStarUpload";
    }

}
