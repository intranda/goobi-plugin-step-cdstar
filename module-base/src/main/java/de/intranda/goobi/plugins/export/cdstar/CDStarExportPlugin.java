package de.intranda.goobi.plugins.export.cdstar;

import java.util.HashMap;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.goobi.api.mq.QueueType;
import org.goobi.api.mq.TaskTicket;
import org.goobi.api.mq.TicketGenerator;
import org.goobi.beans.GoobiProperty;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginGuiType;
import org.goobi.production.enums.PluginReturnValue;
import org.goobi.production.enums.PluginType;
import org.goobi.production.enums.StepReturnValue;
import org.goobi.production.plugin.interfaces.IStepPluginVersion2;

import de.sub.goobi.config.ConfigPlugins;
import jakarta.jms.JMSException;
import lombok.Getter;
import lombok.Setter;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
public class CDStarExportPlugin implements IStepPluginVersion2 {

    private static final long serialVersionUID = 733853438595946732L;

    @Getter
    private String title = "intranda_step_cdstarExport";

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
        return null; //NOSONAR
    }

    @Override
    public int getInterfaceVersion() {
        return 0;
    }

    @Override
    public PluginReturnValue run() {

        TaskTicket exportTicket = TicketGenerator.generateSimpleTicket("CDStarExport");

        exportTicket.setProcessId(step.getProzess().getId());
        exportTicket.setProcessName(step.getProzess().getTitel());

        exportTicket.setStepId(step.getId());
        exportTicket.setStepName(step.getTitel());

        SubnodeConfiguration xmlConfig = ConfigPlugins.getProjectAndStepConfig("intranda_step_cdstarIngest", step);

        String cdstarUrl = xmlConfig.getString("url");
        String vault = xmlConfig.getString("vault");
        String user = xmlConfig.getString("user");
        String password = xmlConfig.getString("password");

        exportTicket.getProperties().put("userName", user);
        exportTicket.getProperties().put("password", password);

        exportTicket.getProperties().put("url", cdstarUrl);
        exportTicket.getProperties().put("vault", vault);

        exportTicket.getProperties().put("closeStep", "true");

        String archiveName = "";
        for (GoobiProperty prop : step.getProzess().getEigenschaften()) {
            if ("archive-id".equals(prop.getTitel())) {
                archiveName = prop.getWert();
                break;
            }
        }

        exportTicket.getProperties().put("archiveurl", cdstarUrl + vault + "/" + archiveName);

        try {
            TicketGenerator.submitInternalTicket(exportTicket, QueueType.SLOW_QUEUE, "cdstarexport", step.getProcessId());
        } catch (JMSException e) {
            return PluginReturnValue.ERROR;
        }

        return PluginReturnValue.WAIT;
    }

}
