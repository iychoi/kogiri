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
package kogiri.mapreduce.preprocess.common.kmerindex;

import java.io.File;
import java.io.IOException;
import kogiri.common.json.JsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerIndexInputFormatConfig {
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.preprocess.common.kmerindex.kmerindexinputformatconfig";
    
    private int kmerSize;
    private String kmerIndexIndexPath;
    
    public static KmerIndexInputFormatConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonFile(file, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJson(json, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerIndexInputFormatConfig.class);
    }
    
    public static KmerIndexInputFormatConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexInputFormatConfig) serializer.fromJsonFile(fs, file, KmerIndexInputFormatConfig.class);
    }
    
    public KmerIndexInputFormatConfig() {
        
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmerindex_index_path")
    public void setKmerIndexIndexPath(String kmerIndexIndexPath) {
        this.kmerIndexIndexPath = kmerIndexIndexPath;
    }
    
    @JsonProperty("kmerindex_index_path")
    public String getKmerIndexIndexPath() {
        return this.kmerIndexIndexPath;
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
