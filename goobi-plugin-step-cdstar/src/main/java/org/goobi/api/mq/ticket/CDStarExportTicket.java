package org.goobi.api.mq.ticket;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketHandler;
import org.goobi.beans.Process;
import org.goobi.beans.Project;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginReturnValue;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import de.sub.goobi.export.dms.ExportDms;
import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.ExportFileException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.helper.exceptions.UghHelperException;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.extern.log4j.Log4j;
import ugh.exceptions.DocStructHasNoTypeException;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;

@Log4j
public class CDStarExportTicket extends ExportDms implements TicketHandler<PluginReturnValue> {

    private static final Namespace metsNamespace = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    private static final Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");

    @Override
    public PluginReturnValue call(TaskTicket ticket) {
        log.info("got export ticket for " + ticket.getProcessName());

        String userName = ticket.getProperties().get("userName");
        String password = ticket.getProperties().get("password");
        String archiveurl = ticket.getProperties().get("archiveurl");
        Client client = ClientBuilder.newClient().register(new BasicAuthenticator(userName, password));

        WebTarget archiveBase = client.target(archiveurl);
        ArchiveInformation data =
                archiveBase.queryParam("with", "files").queryParam("limit", "1000").request(MediaType.APPLICATION_JSON).get(ArchiveInformation.class);
        int position = 1000;

        boolean filesFound = true;
        while (filesFound) {

            ArchiveInformation sublist = archiveBase.queryParam("with", "files")
                    .queryParam("limit", "1000")
                    .queryParam("offset", position)
                    .request(MediaType.APPLICATION_JSON)
                    .get(ArchiveInformation.class);
            List<FileInformation> additionalFiles = sublist.getFiles();
            if (additionalFiles == null || additionalFiles.isEmpty()) {
                filesFound = false;
            } else {
                data.getFiles().addAll(additionalFiles);
                position = position + 1000;
            }
        }

        // export process
        Process process = ProcessManager.getProcessById(ticket.getProcessId());
        String exportFolder = "/opt/gigiverso/goobi/export/";
        Project project = process.getProjekt();
        project.setUseDmsImport(false);

        try {
            exportWithImages = false;
            startExport(process, exportFolder);
        } catch (WriteException | PreferencesException | DocStructHasNoTypeException | MetadataTypeNotAllowedException
                | TypeNotAllowedForParentException | IOException | InterruptedException | ExportFileException | UghHelperException | SwapException
                | DAOException e) {
            log.error(e);
            return PluginReturnValue.ERROR;
        }

        Path metsFile = null;
        if (project.isDmsImportCreateProcessFolder()) {
            metsFile = Paths.get(exportFolder, process.getTitel(), process.getTitel() + ".xml");
        } else {
            metsFile = Paths.get(exportFolder, process.getTitel() + ".xml");
        }

        // read exported file and create/overwrite filegroup for cdstar
        if (createAdditionalFileGroups(archiveurl, data, metsFile.toString())) {
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
        }

        return PluginReturnValue.FINISH;
    }

    private boolean createAdditionalFileGroups(String archiveurl, ArchiveInformation data, String metsFilename) {
        List<FileInformation> master = new ArrayList<>();
        List<FileInformation> derivates = new ArrayList<>();

        List<FileInformation> alto = new ArrayList<>();
        List<FileInformation> pdf = new ArrayList<>();
        List<FileInformation> txt = new ArrayList<>();

        for (FileInformation fi : data.getFiles()) {
            if (fi.getName().contains("/master/")) {
                master.add(fi);
            } else if (fi.getName().contains("/ocr/") && fi.getName().endsWith(".pdf")) {
                pdf.add(fi);
            } else if (fi.getName().contains("/ocr/") && fi.getName().endsWith(".txt")) {
                txt.add(fi);
            } else if (fi.getName().contains("/ocr/") && fi.getName().endsWith(".xml")) {
                alto.add(fi);
            } else {
                derivates.add(fi);
            }
        }

        SAXBuilder parser = new SAXBuilder();
        Document metsDoc = null;
        try {
            metsDoc = parser.build(metsFilename);
        } catch (JDOMException | IOException e) {
            Helper.setFehlerMeldung("error while parsing mets file");
            log.error("error while parsing mets file", e);
            return false;
        }
        Element metsRoot = metsDoc.getRootElement();

        Element fileSec = metsRoot.getChild("fileSec", metsNamespace);
        List<Element> filegroups = fileSec.getChildren("fileGrp", metsNamespace);
        for (Element filegroup : filegroups) {
            if (filegroup.getAttributeValue("USE").equals("CDSTAR")) {
                generateFileGroup(archiveurl, master, filegroup);
            } else if (filegroup.getAttributeValue("USE").equals("DERIVATE")) {
                generateFileGroup(archiveurl, derivates, filegroup);
            } else if (filegroup.getAttributeValue("USE").equals("ALTO")) {
                generateFileGroup(archiveurl, alto, filegroup);
            } else if (filegroup.getAttributeValue("USE").equals("PDF")) {
                generateFileGroup(archiveurl, pdf, filegroup);
            } else if (filegroup.getAttributeValue("USE").equals("TXT")) {
                generateFileGroup(archiveurl, txt, filegroup);
            }
        }

        List<Element> structMapList = metsRoot.getChildren("structMap", metsNamespace);

        for (Element structMap : structMapList) {
            if (structMap.getAttributeValue("TYPE").equals("PHYSICAL")) {
                Element physSequence = structMap.getChild("div", metsNamespace);

                List<Element> divs = physSequence.getChildren("div", metsNamespace);

                for (int i = 0; i < divs.size(); i++) {
                    Element div = divs.get(i);

                    List<Element> fptrList = div.getChildren("fptr", metsNamespace);
                    for (Element fptr : fptrList) {
                        if (fptr.getAttributeValue("FILEID").contains("CDSTAR")) {
                            fptr.setAttribute("CONTENTIDS", master.get(i).getId());
                        } else if (fptr.getAttributeValue("FILEID").contains("DERIVATE")) {
                            fptr.setAttribute("CONTENTIDS", derivates.get(i).getId());
                        } else if (fptr.getAttributeValue("FILEID").contains("ALTO")) {
                            fptr.setAttribute("CONTENTIDS", alto.get(i).getId());
                        } else if (fptr.getAttributeValue("FILEID").contains("TXT")) {
                            fptr.setAttribute("CONTENTIDS", txt.get(i).getId());
                        } else if (fptr.getAttributeValue("FILEID").contains("PDF")) {
                            fptr.setAttribute("CONTENTIDS", pdf.get(i).getId());
                        }
                    }
                }
            }
        }
        XMLOutputter outputter = new XMLOutputter(Format.getPrettyFormat());
        try {
            FileOutputStream output = new FileOutputStream(metsFilename);
            outputter.output(metsDoc, output);
        } catch (IOException e) {
            Helper.setFehlerMeldung("error while writing mets file");
            log.error("error while writing mets file", e);
            return false;
        }
        return true;
    }

    private void generateFileGroup(String archiveurl, List<FileInformation> master, Element filegroup) {
        List<Element> metsFiles = filegroup.getChildren("file", metsNamespace);
        for (int i = 0; i < metsFiles.size(); i++) {
            Element file = metsFiles.get(i);

            file.setAttribute("MIMETYPE", master.get(i).getType());

            Element flocat = file.getChild("FLocat", metsNamespace);
            Attribute href = flocat.getAttribute("href", xlink);
            href.setValue(archiveurl + "/" + master.get(i).getName());
        }
    }

    @Override
    public String getTicketHandlerName() {
        return "CDStarExport";
    }

}
