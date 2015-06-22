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
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
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
public class KmerMatchResult {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchResult.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.readfrequency.common.kmermatch.kmermatchresult";
    
    private CompressedSequenceWritable key;
    private CompressedIntArrayWritable[] vals;
    private Path[] kmerIndexPath;
    
    public static KmerMatchResult createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonFile(file, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJson(json, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchResult.class);
    }
    
    public static KmerMatchResult createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchResult) serializer.fromJsonFile(fs, file, KmerMatchResult.class);
    }
    
    public KmerMatchResult() {
        
    }
    
    public KmerMatchResult(CompressedSequenceWritable key, CompressedIntArrayWritable[] vals, Path[] kmerIndexPath) {
        this.key = key;
        this.vals = vals;
        this.kmerIndexPath = kmerIndexPath;
    }
    
    @JsonProperty("kmer")
    public String getKmer() {
        return this.key.getSequence();
    }
    
    @JsonIgnore
    public CompressedSequenceWritable getKey() {
        return this.key;
    }
    
    @JsonIgnore
    public CompressedIntArrayWritable[] getVals() {
        return this.vals;
    }
    
    @JsonProperty("kmer_index_path")
    public Path[] getKmerIndexPath() {
        return this.kmerIndexPath;
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
