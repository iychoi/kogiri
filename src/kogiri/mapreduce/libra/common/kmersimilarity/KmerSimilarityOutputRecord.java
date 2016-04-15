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
package kogiri.mapreduce.libra.common.kmersimilarity;

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
public class KmerSimilarityOutputRecord {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityOutputRecord.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.libra.common.kmersimilarity.kmersimilarityoutputrecord";
    
    private double[] similarityScore;
    
    public static KmerSimilarityOutputRecord createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonFile(file, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJson(json, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerSimilarityOutputRecord.class);
    }
    
    public static KmerSimilarityOutputRecord createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerSimilarityOutputRecord) serializer.fromJsonFile(fs, file, KmerSimilarityOutputRecord.class);
    }
    
    public KmerSimilarityOutputRecord() {
    }
    
    @JsonProperty("score")
    public double[] getScore() {
        return this.similarityScore;
    }
    
    @JsonProperty("score")
    public void setScore(double[] score) {
        this.similarityScore = score;
    }
    
    @JsonIgnore
    public void addScore(double[] score) {
        if(this.similarityScore == null) {
            this.similarityScore = score;
        }
        
        for(int i=0;i<score.length;i++) {
            this.similarityScore[i] += score[i];
        }
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
