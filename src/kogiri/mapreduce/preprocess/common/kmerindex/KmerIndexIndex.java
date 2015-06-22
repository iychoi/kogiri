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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
public class KmerIndexIndex {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexIndex.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.preprocess.common.kmerindex.kmerindexindex";
    
    private List<String> lastKeyList = new ArrayList<String>();
    
    public static KmerIndexIndex createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonFile(file, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJson(json, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerIndexIndex.class);
    }
    
    public static KmerIndexIndex createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerIndexIndex) serializer.fromJsonFile(fs, file, KmerIndexIndex.class);
    }
    
    public KmerIndexIndex() {
    }
    
    @JsonIgnore
    public void addLastKey(String key) {
        this.lastKeyList.add(key);
    }
    
    @JsonProperty("last_keys")
    public void addLastKey(Collection<String> keys) {
        this.lastKeyList.addAll(keys);
    }
    
    @JsonIgnore
    public Collection<String> getLastKey() {
        return Collections.unmodifiableCollection(this.lastKeyList);
    }
    
    @JsonProperty("last_keys")
    public Collection<String> getSortedLastKeys() {
        Collections.sort(this.lastKeyList);
        return Collections.unmodifiableCollection(this.lastKeyList);
    }
    
    @JsonIgnore
    public int getSize() {
        return this.lastKeyList.size();
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
