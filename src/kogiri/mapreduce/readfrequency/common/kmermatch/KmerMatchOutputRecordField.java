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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class KmerMatchOutputRecordField {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchOutputRecordField.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.readfrequency.common.kmermatch.kmermatchoutputrecordfield";
    
    private int fileid;
    private ArrayList<Integer> readids = new ArrayList<Integer>();
    
    public static KmerMatchOutputRecordField createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchOutputRecordField) serializer.fromJsonFile(file, KmerMatchOutputRecordField.class);
    }
    
    public static KmerMatchOutputRecordField createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchOutputRecordField) serializer.fromJson(json, KmerMatchOutputRecordField.class);
    }
    
    public static KmerMatchOutputRecordField createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchOutputRecordField) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerMatchOutputRecordField.class);
    }
    
    public static KmerMatchOutputRecordField createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerMatchOutputRecordField) serializer.fromJsonFile(fs, file, KmerMatchOutputRecordField.class);
    }
    
    public KmerMatchOutputRecordField() {
        
    }
    
    @JsonProperty("file_id")
    public int getFileID() {
        return this.fileid;
    }
    
    @JsonProperty("file_id")
    public void setFileID(int fileid) {
        this.fileid = fileid;
    }
    
    @JsonProperty("read_ids")
    public Collection<Integer> getReadIDs() {
        return Collections.unmodifiableCollection(this.readids);
    }
    
    @JsonProperty("read_ids")
    public void addReadIDs(Collection<Integer> readids) {
        this.readids.addAll(readids);
    }
    
    @JsonIgnore
    public void addReadIDs(int[] readids) {
        for(int readid : readids) {
            addReadID(readid);
        }
    }
    
    @JsonIgnore
    public void addReadID(int readid) {
        this.readids.add(readid);
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
