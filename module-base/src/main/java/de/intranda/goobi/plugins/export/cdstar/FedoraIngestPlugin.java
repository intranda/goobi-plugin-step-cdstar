package de.intranda.goobi.plugins.export.cdstar;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import javax.jms.JMSException;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.goobi.api.mq.QueueType;
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

    private static final long serialVersionUID = -4980583981253950557L;

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
        run();
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
        return null;//NOSONAR
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

        SubnodeConfiguration xmlConfig = ConfigPlugins.getProjectAndStepConfig("intranda_step_cdstarIngest", step);

        String fedoraUrl = xmlConfig.getString("fedoraUrl");
        exportTicket.getProperties().put("fedoraUrl", fedoraUrl);
        exportTicket.getProperties().put("closeStep", "true");

        String exportFolder = step.getProzess().getProjekt().getDmsImportRootPath();

        Path metsFile = null;
        metsFile = Paths.get(exportFolder, step.getProzess().getTitel(), step.getProzess().getTitel() + ".xml");

        exportTicket.getProperties().put("metsfile", metsFile.toString());

        try {
            TicketGenerator.submitInternalTicket(exportTicket, QueueType.SLOW_QUEUE, "fedoraingest", step.getProcessId());
        } catch (JMSException e) {
            return PluginReturnValue.ERROR;
        }

        return PluginReturnValue.WAIT;
    }

}
