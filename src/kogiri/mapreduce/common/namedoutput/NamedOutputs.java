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
package kogiri.mapreduce.common.namedoutput;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
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
public class NamedOutputs {
    
    private static final Log LOG = LogFactory.getLog(NamedOutputs.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.common.namedoutputs";
    
    private Hashtable<String, Integer> identifierCache = new Hashtable<String, Integer>();
    private Hashtable<String, Integer> filenameCache = new Hashtable<String, Integer>();
    private List<NamedOutputRecord> recordList = new ArrayList<NamedOutputRecord>();
    
    public static NamedOutputs createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJsonFile(file, NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJson(json, NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (NamedOutputs) serializer.fromJson(conf.get(HADOOP_CONFIG_KEY), NamedOutputs.class);
    }
    
    public static NamedOutputs createInstance(Path file, FileSystem fs) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        DataInputStream reader = fs.open(file);
        
        String jsonString = Text.readString(reader);
        reader.close();
        
        return (NamedOutputs) serializer.fromJson(jsonString, NamedOutputs.class);
    }
    
    public NamedOutputs() {
    }
    
    @JsonIgnore
    public void add(Path file) {
        add(file.getName());
    }
    
    @JsonIgnore
    public void add(Path[] files) {
        for(Path file : files) {
            add(file.getName());
        }
    }
    
    @JsonIgnore
    public void add(String[] filenames) {
        for(String filename : filenames) {
            add(filename);
        }
    }
    
    @JsonIgnore
    public void add(String filename) {
        String identifier = getSafeIdentifier(filename);
        
        if(this.identifierCache.get(identifier) == null) {
            try {
                // okey
                addRecord(identifier, filename);
            } catch (DuplicatedNamedOutputException ex) {
                LOG.error(ex);
            }
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String identifierTrial = getSafeIdentifier(filename + trial);
                if(this.identifierCache.get(identifierTrial) == null) {
                    // okey
                    try {
                        addRecord(identifierTrial, filename);
                    } catch (DuplicatedNamedOutputException ex) {
                        LOG.error(ex);
                    }
                    success = true;
                    break;
                }
            }
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecord(String identifier) {
        Integer ret = this.identifierCache.get(identifier);
        if(ret == null) {
            return null;
        } else {
            return this.recordList.get(ret.intValue());
        }
    }
    
    @JsonIgnore
    public void addRecord(NamedOutputRecord record) throws DuplicatedNamedOutputException {
        addRecord(record.getIdentifier(), record.getFilename());
    }
    
    @JsonIgnore
    public void addRecord(String identifier, String filename) throws DuplicatedNamedOutputException {
        if(this.identifierCache.get(identifier) == null) {
            // okey
            NamedOutputRecord record = new NamedOutputRecord(identifier, filename);
            this.identifierCache.put(identifier, this.recordList.size());
            this.filenameCache.put(filename, this.recordList.size());
            this.recordList.add(record);
        } else {
            throw new DuplicatedNamedOutputException("duplicated named output record (" + identifier + ") is found");
        }
    }
    
    @JsonProperty("records")
    public Collection<NamedOutputRecord> getRecord() {
        return Collections.unmodifiableList(this.recordList);
    }
    
    @JsonProperty("records")
    public void addRecord(Collection<NamedOutputRecord> records) throws DuplicatedNamedOutputException {
        for(NamedOutputRecord record : records) {
            addRecord(record);
        }
    }
    
    @JsonIgnore
    public int getIDFromFilename(String filename) throws IOException {
        Integer ret = this.filenameCache.get(filename);
        if(ret == null) {
            throw new IOException("could not find id from " + filename);
        } else {
            return ret.intValue();
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromID(int id) throws IOException {
        if(this.recordList.size() <= id) {
            throw new IOException("could not find record " + id);
        } else {
            return this.recordList.get(id);    
        }
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromMROutput(Path outputFile) throws IOException {
        return getRecordFromMROutput(outputFile.getName());
    }
    
    @JsonIgnore
    public NamedOutputRecord getRecordFromMROutput(String outputFilename) throws IOException {
        String identifier = MapReduceHelper.getOutputNameFromMapReduceOutput(outputFilename);
        Integer ret = this.identifierCache.get(identifier);
        if(ret == null) {
            throw new IOException("could not find record " + outputFilename);
        } else {
            return this.recordList.get(ret.intValue());
        }
    }
    
    @JsonIgnore
    public int getSize() {
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
    
    @JsonIgnore
    public static String getSafeIdentifier(String filename) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : filename.toCharArray()) {
            boolean isSafe = false;
            if ((ch >= 'A') && (ch <= 'Z')) {
                isSafe = true;
            } else if ((ch >= 'a') && (ch <= 'z')) {
                isSafe = true;
            } else if ((ch >= '0') && (ch <= '9')) {
                isSafe = true;
            }
            
            if(isSafe) {
                sb.append(ch);
            }
        }
        
        return sb.toString();
    }
}
