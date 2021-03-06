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
package kogiri.hadoop.common.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import kogiri.common.helpers.JarResourceHelper;
import kogiri.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ClusterConfiguration {

    private static final Log LOG = LogFactory.getLog(ClusterConfiguration.class);
    private static final String PREDEFINED_CLUSTER_CONFIG_PATH = "/config/cluster/";
    
    private int mrVersion = 2;
    private int machineCores = 1;
    private int machineNum = 1;
    
    private ArrayList<ConfigurationParam> externalParams = new ArrayList<ConfigurationParam>();
    
    public static ClusterConfiguration createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterConfiguration) serializer.fromJsonFile(file, ClusterConfiguration.class);
    }
    
    public static ClusterConfiguration createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterConfiguration) serializer.fromJson(json, ClusterConfiguration.class);
    }
    
    public static ClusterConfiguration createInstanceFromPredefined(String configurationName) throws IOException {
        if(JarResourceHelper.hasResource(PREDEFINED_CLUSTER_CONFIG_PATH + configurationName.toLowerCase() + ".json")) {
            String jsonConfig = JarResourceHelper.getResourceAsText(PREDEFINED_CLUSTER_CONFIG_PATH + configurationName.toLowerCase() + ".json");
            return createInstance(jsonConfig);
        } else {
            throw new IOException("file not found");
        }
    }
    
    public ClusterConfiguration() {
        
    }
    
    @JsonProperty("machine_cores")
    public int getMachineCores() {
        return this.machineCores;
    }
    
    @JsonProperty("machine_cores")
    public void setMachineCores(int machine_cores) {
        this.machineCores = machine_cores;
    }
    
    @JsonProperty("machine_num")
    public int getMachineNum() {
        return this.machineNum;
    }
    
    @JsonProperty("machine_num")
    public void setMachineNum(int machine_num) {
        this.machineNum = machine_num;
    }
    
    @JsonProperty("mr_version")
    public int getMapReduceVersion() {
        return this.mrVersion;
    }
    
    @JsonProperty("mr_version")
    public void setMapReduceVersion(int version) {
        this.mrVersion = version;
    }
    
    @JsonProperty("external_param")
    public Collection<ConfigurationParam> getExternalParam() {
        return this.externalParams;
    } 
    
    @JsonProperty("external_param")
    public void addExernalParam(Collection<ConfigurationParam> params) {
        this.externalParams.addAll(params);
    }
    
    @JsonIgnore
    public void addExternalParam(ConfigurationParam param) {
        this.externalParams.add(param);
    }
    
    @JsonIgnore
    public void configureTo(Configuration conf) {
        for(ConfigurationParam param : this.externalParams) {
            if(param.isValueInt()) {
                conf.setInt(param.getKey(), param.getValueAsInt());
            } else {
                conf.set(param.getKey(), param.getValue());
            }
        }
    }
    
    @JsonIgnore
    public void configureTo(SparkConf conf) {
        for(ConfigurationParam param : this.externalParams) {
            conf.set(param.getKey(), param.getValue());
        }
    }
    
    @JsonIgnore
    @Override
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        sb.append("MapReduceVersion = " + mrVersion);
        sb.append("\n");
        sb.append("CoresPerMachine = " + machineCores);
        sb.append("\n");
        sb.append("MachineNumInCluster = " + machineNum);
        
        for(ConfigurationParam param : this.externalParams) {
            sb.append("\n");
            sb.append(param.getKey() + " = " + param.getValue());
        }
        
        return sb.toString();
    }
}
