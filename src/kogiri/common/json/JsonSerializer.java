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
package kogiri.common.json;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 *
 * @author iychoi
 */
public class JsonSerializer {
    
    private ObjectMapper mapper;
            
    public JsonSerializer() {
        this.mapper = new ObjectMapper();
    }
    
    public JsonSerializer(boolean prettyformat) {
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, prettyformat);
    }
    
    public String toJson(Object obj) throws IOException {
        StringWriter writer = new StringWriter();
        this.mapper.writeValue(writer, obj);
        return writer.getBuffer().toString();
    }
    
    public void toJsonConfiguration(Configuration conf, String key, Object obj) throws IOException {
        String jsonString = toJson(obj);
        
        conf.set(key, jsonString);
    }
    
    public void toJsonConfiguration(SparkConf conf, String key, Object obj) throws IOException {
        String jsonString = toJson(obj);
        
        conf.set(key, jsonString);
    }
    
    public void toJsonFile(File f, Object obj) throws IOException {
        this.mapper.writeValue(f, obj);
    }
    
    public void toJsonFile(FileSystem fs, Path file, Object obj) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream ostream = fs.create(file, true, 64 * 1024, (short)3, 1024 * 1024);
        this.mapper.writeValue(ostream, obj);
        ostream.close();
    }
    
    public Object fromJson(String json, Class<?> cls) throws IOException {
        if(json == null) {
            return null;
        }
        StringReader reader = new StringReader(json);
        return this.mapper.readValue(reader, cls);
    }
    
    public Object fromJsonConfiguration(Configuration conf, String key, Class<?> cls) throws IOException {
        String jsonString = conf.get(key);
        
        if(jsonString == null) {
            return null;
        }
        
        return fromJson(jsonString, cls);
    }
    
    public Object fromJsonFile(File f, Class<?> cls) throws IOException {
        return this.mapper.readValue(f, cls);
    }
    
    public Object fromJsonFile(FileSystem fs, Path file, Class<?> cls) throws IOException {
        DataInputStream istream = fs.open(file);
        Object obj = this.mapper.readValue(istream, cls);
        
        istream.close();
        return obj;
    }
}
