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
package kogiri.mapreduce.preprocess.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import kogiri.common.helpers.PathHelper;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.common.config.ClusterConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class PreprocessorConfig {
    
    public static final int DEFAULT_KMERSIZE = 20;
    public static final String DEFAULT_OUTPUT_ROOT_PATH = "./kogiri_preprocess_output";
    public static final String DEFAULT_KMER_HISTOGRAM_PATH = "histogram";
    public static final String DEFAULT_READ_INDEX_PATH = "readindex";
    public static final String DEFAULT_KMER_INDEX_PATH = "kmerindex";
    public static final String DEFAULT_STATISITCS_PATH = "statistics";
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.preprocess.preprocessorconfig";
    
    private ClusterConfiguration clusterConfiguration = new ClusterConfiguration();
    private String reportFilePath;
    
    private int kmerSize = DEFAULT_KMERSIZE;
    private ArrayList<String> fastaPaths = new ArrayList<String>();
    private String kmerHistogramPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_HISTOGRAM_PATH;
    private String readIndexPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_READ_INDEX_PATH;
    private String kmerIndexPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_KMER_INDEX_PATH;
    private String statisticsPath = DEFAULT_OUTPUT_ROOT_PATH + "/" + DEFAULT_STATISITCS_PATH;
    
    public static PreprocessorConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJsonFile(file, PreprocessorConfig.class);
    }
    
    public static PreprocessorConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJson(json, PreprocessorConfig.class);
    }
    
    public static PreprocessorConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (PreprocessorConfig) serializer.fromJson(conf.get(HADOOP_CONFIG_KEY), PreprocessorConfig.class);
    }
    
    public PreprocessorConfig() {
        
    }

    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }

    @JsonProperty("fasta_path")
    public Collection<String> getFastaPath() {
        return this.fastaPaths;
    }
    
    @JsonProperty("fasta_path")
    public void addFastaPath(Collection<String> fastaPaths) {
        this.fastaPaths.addAll(fastaPaths);
    }
    
    @JsonIgnore
    public void addFastaPath(String fastaPath) {
        this.fastaPaths.add(fastaPath);
    }

    @JsonIgnore
    public void setOutputRootPath(String outputRootPath) {
        this.kmerHistogramPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_HISTOGRAM_PATH);
        this.readIndexPath = PathHelper.concatPath(outputRootPath, DEFAULT_READ_INDEX_PATH);
        this.kmerIndexPath = PathHelper.concatPath(outputRootPath, DEFAULT_KMER_INDEX_PATH);
        this.statisticsPath = PathHelper.concatPath(outputRootPath, DEFAULT_STATISITCS_PATH);
    }
    
    @JsonProperty("histogram_path")
    public String getKmerHistogramPath() {
        return this.kmerHistogramPath;
    }
    
    @JsonProperty("histogram_path")
    public void setKmerHistogramPath(String histogramPath) {
        this.kmerHistogramPath = histogramPath;
    }
    
    @JsonProperty("read_index_path")
    public String getReadIndexPath() {
        return this.readIndexPath;
    }
    
    @JsonProperty("read_index_path")
    public void setReadIndexPath(String readIndexPath) {
        this.readIndexPath = readIndexPath;
    }
    
    @JsonProperty("kmer_index_path")
    public String getKmerIndexPath() {
        return this.kmerIndexPath;
    }
    
    @JsonProperty("kmer_index_path")
    public void setKmerIndexPath(String kmerIndexPath) {
        this.kmerIndexPath = kmerIndexPath;
    }
    
    @JsonProperty("statistics_path")
    public String getStatisticsPath() {
        return this.statisticsPath;
    }
    
    @JsonProperty("statistics_path")
    public void setStatisticsPath(String statisticsPath) {
        this.statisticsPath = statisticsPath;
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
        String jsonString = serializer.toJson(this);
        
        conf.set(HADOOP_CONFIG_KEY, jsonString);
    }
}
