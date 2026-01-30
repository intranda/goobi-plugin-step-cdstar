package de.intranda.goobi.plugins.export.cdstar;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import de.sub.goobi.helper.StorageProvider;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Converter {

    private static final boolean useOldTable = true;

    private static final String dbUrl = "jdbc:mysql://localhost:3306/mpiwg";
    private static final String dbUser = "user";
    private static final String dbPwd = "password";

    private static final Namespace metsNs = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    private static final Namespace modsNs = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    private static final Namespace goobiNs = Namespace.getNamespace("goobi", "http://meta.goobi.org/v1.5.1/");

    public static void main(String[] args) {

        // find all process folder with an existing meta.xml

        List<Path> files = new ArrayList<>();
        try {
            Files.find(Paths.get("/opt/digiverso/goobi/metadata/"), 2,
                    (p, file) -> file.isRegularFile() && p.getFileName().toString().matches("meta.xml")).forEach(p -> files.add(p));
        } catch (IOException e) {
            log.error(e);
        }
        PreparedStatement stmnt = null;
        try {
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
            // for each record: search in DB for the latest 'archive-id' process property (prozesseeigenschaften or properties table)
            if (useOldTable) {
                stmnt = connection.prepareStatement(
                        "select wert from prozesseeigenschaften where prozesseeigenschaftenid =(select max(prozesseeigenschaftenid) from prozesseeigenschaften where titel = 'archive-id' and prozesseID = ?)");
            } else {
                stmnt = connection.prepareStatement(
                        "select property_value from properties where id = (select max(id) from properties where property_name = 'archive-id' and object_id=?)");
            }
        } catch (SQLException e) {
            log.error(e);
        }

        SAXBuilder builder = new SAXBuilder();
        // Disable access to external entities
        builder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        builder.setFeature("http://xml.org/sax/features/external-general-entities", false);
        builder.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        for (Path metsfile : files) {
            String processid = metsfile.getParent().getFileName().toString();
            try {

                // get archive-id from database
                stmnt.setString(1, processid);
                ResultSet resultS = stmnt.executeQuery();
                while (!resultS.isLast()) {
                    resultS.next();
                    String archiveId = resultS.getString(1);
                    System.out.println(archiveId);
                    // open meta.xml
                    Document doc = builder.build(metsfile.toFile());

                    // store property value in file
                    Element mets = doc.getRootElement();

                    Element dmdSec = mets.getChildren("dmdSec", metsNs).get(0);
                    Element ext = dmdSec.getChild("mdWrap", metsNs)
                            .getChild("xmlData", metsNs)
                            .getChild("mods", modsNs)
                            .getChild("extension", modsNs)
                            .getChild("goobi", goobiNs);

                    Element metadata = new Element("metadata", goobiNs);
                    metadata.setAttribute("name", "CDStarID");
                    metadata.setText(archiveId);
                    ext.addContent(metadata);

                    // save meta.xml
                    try (OutputStream out = StorageProvider.getInstance().newOutputStream(metsfile)) {
                        XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());
                        xmlOutputter.output(doc, out);
                    }
                }
            } catch (SQLException | JDOMException | IOException e) {
                log.error(e);
            }
        }
    }
}
