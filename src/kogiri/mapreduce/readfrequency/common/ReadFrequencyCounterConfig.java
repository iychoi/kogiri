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
package kogiri.mapreduce.readfrequency.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import kogiri.common.helpers.PathHelper;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.common.config.ClusterConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ReadFrequencyCounterConfig {
    
    public static final String DEFAULT_OUTPUT_ROOT_PATH = "./kogiri_readfrequency_output";
    public static final String DEFAULT_KMER_MATCH_PATH = "match";
    public static final String DEFAULT_READ_FREQUENCY_PATH = "readfrequency";
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.readfrequency.common.readfrequencycounterconfig";
    
    private ClusterConfiguration clusterConfiguration = new ClusterConfiguration();
    private String reportFilePath;
    
    private ArrayList<String> kmerIndexPaths = new ArrayList<String>();
    private String kmerHistogramPath;
    private String kmerStatisticsPath;
    private String kmerMatchPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_MATCH_PATH;
    private String readFrequencyPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_READ_FREQUENCY_PATH;
    
    public static ReadFrequencyCounterConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ReadFrequencyCounterConfig) serializer.fromJsonFile(file, ReadFrequencyCounterConfig.class);
    }
    
    public static ReadFrequencyCounterConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ReadFrequencyCounterConfig) serializer.fromJson(json, ReadFrequencyCounterConfig.class);
    }
    
    public static ReadFrequencyCounterConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ReadFrequencyCounterConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, ReadFrequencyCounterConfig.class);
    }
    
    public static ReadFrequencyCounterConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ReadFrequencyCounterConfig) serializer.fromJsonFile(fs, file, ReadFrequencyCounterConfig.class);
    }
    
    public ReadFrequencyCounterConfig() {
        
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

    @JsonIgnore
    public void setOutputRootPath(String outputRootPath) {
        this.kmerMatchPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_MATCH_PATH);
        this.readFrequencyPath = PathHelper.concatPath(outputRootPath, DEFAULT_READ_FREQUENCY_PATH);
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
    
    @JsonProperty("match_path")
    public String getKmerMatchPath() {
        return this.kmerMatchPath;
    }
    
    @JsonProperty("match_path")
    public void setKmerMatchPath(String kmerMatch) {
        this.kmerMatchPath = kmerMatch;
    }
    
    @JsonProperty("read_frequency_path")
    public String getReadFrequencyPath() {
        return this.readFrequencyPath;
    }
    
    @JsonProperty("read_frequency_path")
    public void setReadFrequencyPath(String readFrequency) {
        this.readFrequencyPath = readFrequency;
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
