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
package kogiri.mapreduce.readfrequency.common.kmermatch;

import java.io.File;
import java.io.IOException;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.preprocess.common.kmerstatistics.KmerStandardDeviation;
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
public class KmerMatchInputFormatConfig {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchInputFormatConfig.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.readfrequency.common.kmermatch.kmermatchinputformatconfig";
    
    private int kmerSize;
    private int partitions;
    private String kmerHistogramPath;
    private Class kmerIndexRecordFilterClass;
    private KmerStandardDeviation[] stddev;
    
    public static KmerMatchInputFormatConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonFile(file, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJson(json, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchInputFormatConfig.class);
    }
    
    public static KmerMatchInputFormatConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchInputFormatConfig) serializer.fromJsonFile(fs, file, KmerMatchInputFormatConfig.class);
    }
    
    public KmerMatchInputFormatConfig() {
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_histogram_path")
    public void setKmerHistogramPath(String path) {
        this.kmerHistogramPath = path;
    }
    
    @JsonProperty("kmer_histogram_path")
    public String getKmerHistogramPath() {
        return kmerHistogramPath;
    }
    
    @JsonProperty("partitions")
    public void setPartitionNum(int partitions) {
        this.partitions = partitions;
    }
    
    @JsonProperty("partitions")
    public int getPartitionNum() {
        return this.partitions;
    }
    
    @JsonProperty("kmer_index_record_filter_class")
    public void setKmerIndexRecordFilterClass(String className) {
        try {
            this.kmerIndexRecordFilterClass = Class.forName(className);
        } catch (ClassNotFoundException ex) {
            LOG.error(ex);
        }
    }
    
    @JsonIgnore
    public void setKmerIndexRecordFilterClass(Class clazz) {
        this.kmerIndexRecordFilterClass = clazz;
    }
    
    @JsonProperty("kmer_index_record_filter_class")
    public String getKmerIndexRecordFilterClassString() {
        return this.kmerIndexRecordFilterClass.getCanonicalName();
    }
    
    @JsonIgnore
    public Class getKmerIndexRecordFilterClass() {
        return this.kmerIndexRecordFilterClass;
    }
    
    @JsonProperty("standard_deviation")
    public void setStandardDeviation(KmerStandardDeviation[] stddev) {
        this.stddev = stddev;
    }
    
    @JsonProperty("standard_deviation")
    public KmerStandardDeviation[] getStandardDeviation() {
        return this.stddev;
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
