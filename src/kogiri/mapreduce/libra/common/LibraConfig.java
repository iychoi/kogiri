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
package kogiri.mapreduce.libra.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import kogiri.common.json.JsonSerializer;
import kogiri.hadoop.common.config.ClusterConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 *
 * @author iychoi
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public class LibraConfig {
    
    public static final String DEFAULT_OUTPUT_PATH = "./kogiri_libra_output";
    public static final double DEFAULT_STANDARD_DEVIATION_FACTOR = 2.0;
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.libra.common.libraconfig";
    
    private ClusterConfiguration clusterConfiguration = new ClusterConfiguration();
    private String reportFilePath;
    
    private ArrayList<String> kmerIndexPaths = new ArrayList<String>();
    private String kmerHistogramPath;
    private String kmerStatisticsPath;
    private String outputPath = DEFAULT_OUTPUT_PATH;
    private double stddev_factor = DEFAULT_STANDARD_DEVIATION_FACTOR;
    
    public static LibraConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LibraConfig) serializer.fromJsonFile(file, LibraConfig.class);
    }
    
    public static LibraConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LibraConfig) serializer.fromJson(json, LibraConfig.class);
    }
    
    public static LibraConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LibraConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, LibraConfig.class);
    }
    
    public static LibraConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LibraConfig) serializer.fromJsonFile(fs, file, LibraConfig.class);
    }
    
    public LibraConfig() {
        
    }

    @JsonProperty("kmer_index_path")
    public Collection<String> getKmerIndexPath() {
        return this.kmerIndexPaths;
    }
    
    @JsonProperty("kmer_index_path")
    public void addKmerIndexPath(Collection<String> kmerIndexPaths) {
        this.kmerIndexPaths.addAll(kmerIndexPaths);
    }
    
    @JsonIgnore
    public void addKmerIndexPath(String kmerIndexPath) {
        this.kmerIndexPaths.add(kmerIndexPath);
    }

    @JsonProperty("output_path")
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    
    @JsonProperty("output_path")
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @JsonProperty("histogram_path")
    public String getKmerHistogramPath() {
        return this.kmerHistogramPath;
    }
    
    @JsonProperty("histogram_path")
    public void setKmerHistogramPath(String histogramPath) {
        this.kmerHistogramPath = histogramPath;
    }
    
    @JsonProperty("statistics_path")
    public String getKmerStatisticsPath() {
        return this.kmerStatisticsPath;
    }
    
    @JsonProperty("statistics_path")
    public void setKmerStatisticsPath(String kmerStatisticsPath) {
        this.kmerStatisticsPath = kmerStatisticsPath;
    }
    
    @JsonProperty("standard_deviation_factor")
    public void setStandardDeviationFactor(double factor) {
        this.stddev_factor = factor;
    }
    
    @JsonProperty("standard_deviation_factor")
    public double getStandardDeviationFactor() {
        return this.stddev_factor;
    }
    
    @JsonProperty("cluster_configuration")
    public ClusterConfiguration getClusterConfiguration() {
        return this.clusterConfiguration;
    }
    
    @JsonProperty("cluster_configuration")
    public void setClusterConfiguration(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }

    @JsonProperty("report_path")
    public void setReportPath(String reportFilePath) {
        this.reportFilePath = reportFilePath;
    }
    
    @JsonProperty("report_path")
    public String getReportPath() {
        return this.reportFilePath;
    }
    
    @JsonIgnore
    public void saveTo(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public void saveTo(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
