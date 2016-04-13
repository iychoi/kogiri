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
package kogiri.mapreduce.preprocess.common.kmerstatistics;

import java.io.File;
import java.io.IOException;
import kogiri.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerStatistics {
    
    private static final Log LOG = LogFactory.getLog(KmerStatistics.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.preprocess.common.kmerstatistics.kmerstatistics";
    
    private String sampleName;
    private int kmerSize;
    private long unique;
    private long total;
    private double avgFrequency;
    private double stdDeviation;
    private double tfCosnormBase;
    
    public static KmerStatistics createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonFile(file, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJson(json, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerStatistics.class);
    }
    
    public static KmerStatistics createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatistics) serializer.fromJsonFile(fs, file, KmerStatistics.class);
    }
    
    public KmerStatistics() {
    }
    
    public KmerStatistics(String sampleName, int kmerSize) {
        this.sampleName = sampleName;
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("sample_name")
    public String getSampleName() {
        return this.sampleName;
    }
    
    @JsonProperty("sample_name")
    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("unique_kmers")
    public long getUniqueKmers() {
        return this.unique;
    }
    
    @JsonProperty("unique_kmers")
    public void setUniqueKmers(long uniqueKmers) {
        this.unique = uniqueKmers;
    }
    
    @JsonProperty("total_kmers")
    public long getTotalKmers() {
        return this.total;
    }
    
    @JsonProperty("total_kmers")
    public void setTotalKmers(long totalKmers) {
        this.total = totalKmers;
    }
    
    @JsonProperty("average_frequency")
    public double getAverageFrequency() {
        return this.avgFrequency;
    }
    
    @JsonProperty("average_frequency")
    public void setAverageFrequency(double avgFrequency) {
        this.avgFrequency = avgFrequency;
    }
    
    @JsonProperty("std_deviation")
    public double getStdDeviation() {
        return this.stdDeviation;
    }
    
    @JsonProperty("std_deviation")
    public void setStdDeviation(double stdDeviation) {
        this.stdDeviation = stdDeviation;
    }
    
    @JsonProperty("tf_cosnorm_base")
    public void setTFCosineNormBase(double tfCosnormBase) {
        this.tfCosnormBase = tfCosnormBase;
    }
    
    @JsonProperty("tf_cosnorm_base")
    public double getTFCosineNormBase() {
        return this.tfCosnormBase;
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
