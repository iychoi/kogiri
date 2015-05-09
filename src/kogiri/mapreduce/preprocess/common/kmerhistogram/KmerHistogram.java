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
package kogiri.mapreduce.preprocess.common.kmerhistogram;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import kogiri.common.algorithms.KmerKeySelection;
import kogiri.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerHistogram {
    
    private static final Log LOG = LogFactory.getLog(KmerHistogram.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.common.kmerhistogram";
    
    private static final int SAMPLING_CHARS = 6;
    
    private String sampleName;
    private int kmerSize;
    
    private Hashtable<String, KmerHistogramRecord> recordCache = new Hashtable<String, KmerHistogramRecord>();
    private List<KmerHistogramRecord> recordList = new ArrayList<KmerHistogramRecord>();
    
    private long totalSampleCount = 0;
    private KmerKeySelection keySelectionAlg = new KmerKeySelection();
    
    public static KmerHistogram createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJsonFile(file, KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJson(json, KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerHistogram) serializer.fromJson(conf.get(HADOOP_CONFIG_KEY), KmerHistogram.class);
    }
    
    public static KmerHistogram createInstance(Path file, FileSystem fs) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        DataInputStream reader = fs.open(file);
        
        String jsonString = Text.readString(reader);
        reader.close();
        
        return (KmerHistogram) serializer.fromJson(jsonString, KmerHistogram.class);
    }
    
    public KmerHistogram() {
    }
    
    public KmerHistogram(String sampleName, int kmerSize) {
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
    
    @JsonIgnore
    public void takeSample(String sequence) {
        for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
            // generate kmer
            String kmer = sequence.substring(i, i + this.kmerSize);
            
            String selectedKey = this.keySelectionAlg.selectKey(kmer);
            
            // take first N chars
            String selectedKeySample = selectedKey.substring(0, SAMPLING_CHARS);
            
            add(selectedKeySample);
        }
    }
    
    @JsonIgnore
    private void add(String kmer) {
        KmerHistogramRecord record = this.recordCache.get(kmer);
        if(record == null) {
            record = new KmerHistogramRecord(kmer, 1);
            this.recordCache.put(kmer, record);
            this.recordList.add(record);
        } else {
            record.increaseFrequency();
        }
        
        this.totalSampleCount++;
    }
    
    @JsonIgnore
    public long getTotalSampleCount() {
        return this.totalSampleCount;
    }
    
    @JsonIgnore
    public Collection<KmerHistogramRecord> getRecord() {
        return this.recordList;
    }
    
    @JsonProperty("records")
    public Collection<KmerHistogramRecord> getSortedRecord() {
        Collections.sort(this.recordList, new KmerHistogramRecordComparator());
        return this.recordList;
    }
    
    @JsonProperty("records")
    public void addRecord(Collection<KmerHistogramRecord> records) {
        for(KmerHistogramRecord record : records) {
            addRecord(record);
        }
        
    }
    
    @JsonIgnore
    public void addRecord(KmerHistogramRecord record) {
        KmerHistogramRecord existingRecord = this.recordCache.get(record.getKmer());
        if(existingRecord == null) {
            this.recordCache.put(record.getKmer(), record);
            this.recordList.add(record);
        } else {
            existingRecord.increaseFrequency(record.getFrequency());
        }
        
        this.totalSampleCount += record.getFrequency();
    }
    
    @JsonIgnore
    public int getRecordNum() {
        return this.recordList.size();
    }
    
    @JsonIgnore
    public void saveTo(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        String jsonString = serializer.toJson(this);
        
        conf.set(HADOOP_CONFIG_KEY, jsonString);
    }
    
    @JsonIgnore
    public void saveTo(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        JsonSerializer serializer = new JsonSerializer();
        String jsonString = serializer.toJson(this);
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        new Text(jsonString).write(writer);
        writer.close();
    }
}
