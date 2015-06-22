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
public class KmerStandardDeviation {
    
    private static final Log LOG = LogFactory.getLog(KmerStandardDeviation.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.preprocess.common.kmerstatistics.kmerstandarddeviation";
    
    private double average;
    private double stdDeviation;
    private double factor;
    
    public static KmerStandardDeviation createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStandardDeviation) serializer.fromJsonFile(file, KmerStandardDeviation.class);
    }
    
    public static KmerStandardDeviation createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStandardDeviation) serializer.fromJson(json, KmerStandardDeviation.class);
    }
    
    public static KmerStandardDeviation createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStandardDeviation) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerStandardDeviation.class);
    }
    
    public static KmerStandardDeviation createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStandardDeviation) serializer.fromJsonFile(fs, file, KmerStandardDeviation.class);
    }
    
    public KmerStandardDeviation() {
    }
    
    public KmerStandardDeviation(double avg, double stddeviation, double factor) {
        this.average = avg;
        this.stdDeviation = stddeviation;
        this.factor = factor;
    }
    
    @JsonProperty("average")
    public double getAverage() {
        return this.average;
    }
    
    @JsonProperty("average")
    public void setAverage(double avg) {
        this.average = avg;
    }
    
    @JsonProperty("std_deviation")
    public double getStdDeviation() {
        return this.stdDeviation;
    }
    
    @JsonProperty("std_deviation")
    public void setStdDeviation(double stdDeviation) {
        this.stdDeviation = stdDeviation;
    }
    
    @JsonProperty("factor")
    public double getFactor() {
        return this.factor;
    }
    
    @JsonProperty("factor")
    public void setFactor(double factor) {
        this.factor = factor;
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
