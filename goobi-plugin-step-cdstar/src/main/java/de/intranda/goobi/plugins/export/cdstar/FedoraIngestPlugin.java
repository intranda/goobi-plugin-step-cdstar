package de.intranda.goobi.plugins.export.cdstar;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import javax.jms.JMSException;

import org.apache.commons.configuration.XMLConfiguration;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketGenerator;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginGuiType;
import org.goobi.production.enums.PluginReturnValue;
import org.goobi.production.enums.PluginType;
import org.goobi.production.enums.StepReturnValue;
import org.goobi.production.plugin.interfaces.IStepPluginVersion2;

import de.sub.goobi.config.ConfigPlugins;
import lombok.Getter;
import lombok.Setter;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
public class FedoraIngestPlugin implements IStepPluginVersion2 {

    @Getter
    private String title = "intranda_step_fedoraIngest";

    @Getter
    private PluginType type = PluginType.Step;

    @Getter
    @Setter
    private Step step;

    @Override
    public String cancel() {
        return null;
    }

    @Override
    public boolean execute() {
        return false;
    }

    @Override
    public String finish() {
        return null;
    }

    @Override
    public String getPagePath() {
        return null;
    }

    @Override
    public PluginGuiType getPluginGuiType() {
        return PluginGuiType.NONE;
    }

    @Override
    public void initialize(Step step, String returnPath) {
        this.step = step;

    }

    @Override
    public HashMap<String, StepReturnValue> validate() {
        return null;
    }

    @Override
    public int getInterfaceVersion() {
        return 0;
    }

    @Override
    public PluginReturnValue run() {

        TaskTicket exportTicket = TicketGenerator.generateSimpleTicket("FedoraIngest");

        exportTicket.setProcessId(step.getProzess().getId());
        exportTicket.setProcessName(step.getProzess().getTitel());

        exportTicket.setStepId(step.getId());
        exportTicket.setStepName(step.getTitel());

        XMLConfiguration xmlConfig = ConfigPlugins.getPluginConfig("intranda_step_cdstarIngest");
        //        String cdstarUrl = xmlConfig.getString("url");
        //        String vault = xmlConfig.getString("vault");
        //        String user = xmlConfig.getString("user");
        //        String password = xmlConfig.getString("password");
        //
        //        exportTicket.getProperties().put("userName", user);
        //        exportTicket.getProperties().put("password", password);
        //
        //        exportTicket.getProperties().put("url", cdstarUrl);
        //        exportTicket.getProperties().put("vault", vault);

        String fedoraUrl = xmlConfig.getString("fedoraUrl");
        exportTicket.getProperties().put("fedoraUrl", fedoraUrl);
        exportTicket.getProperties().put("closeStep", "true");

        Path metsFile = null;
        if (step.getProzess().getProjekt().isDmsImportCreateProcessFolder()) {
            metsFile = Paths.get(step.getProzess().getProjekt().getDmsImportImagesPath(), step.getProzess().getTitel(), step.getProzess().getTitel()
                    + ".xml");
        } else {
            metsFile = Paths.get(step.getProzess().getProjekt().getDmsImportImagesPath(), step.getProzess().getTitel() + ".xml");
        }

        exportTicket.getProperties().put("metsfile", metsFile.toString());

        try {
            TicketGenerator.submitTicket(exportTicket, false);
        } catch (JMSException e) {
            return PluginReturnValue.ERROR;
        }

        return PluginReturnValue.WAIT;
    }

}