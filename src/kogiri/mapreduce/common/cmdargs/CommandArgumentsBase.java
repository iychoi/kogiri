/*
 * Copyright (C) 2015 iychoi
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package kogiri.mapreduce.common.cmdargs;

import java.io.File;
import java.io.IOException;
import kogiri.mapreduce.common.config.ClusterConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class CommandArgumentsBase {
    
    private static final Log LOG = LogFactory.getLog(CommandArgumentsBase.class);
    
    @Option(name = "-h", aliases = "--help", usage = "print help message") 
    protected boolean help = false;

    protected ClusterConfiguration cluster = new ClusterConfiguration();

    @Option(name = "-c", aliases = "--clusterconf", usage = "specify cluster configuration")
    public void setClusterConfiguration(String clusterConf) {
        // name of preset? or file?
        try {
            this.cluster = ClusterConfiguration.createInstanceFromPredefined(clusterConf);
            LOG.info("cluster configuration (" + clusterConf + ") is found in predefined settings");
        } catch (IOException ex) {
            try {
                this.cluster = ClusterConfiguration.createInstance(new File(clusterConf));
            } catch (IOException ex1) {
                LOG.error(ex1);
            }
        }
    }
    
    public ClusterConfiguration getClusterConfig() {
        return this.cluster;
    }
    
    @Option(name = "--report", usage = "specify report file to be created")
    protected String reportfile;
    
    public boolean isHelp() {
        return this.help;
    }

    public boolean needReport() {
        return (reportfile != null);
    }
    
    public String getReportFilename() {
        return reportfile;
    }
    
    @Override
    public String toString() {
        return super.toString();
    }
    
    public boolean checkValidity() {
        if(this.cluster == null) {
            return false;
        }
        return true;
    }
}
