package kogiri.mapreduce.readfrequency.modecount;

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
public class ModeCounterConfig {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterConfig.class);
    
    private static final String HADOOP_CONFIG_KEY = "kogiri.mapreduce.readfrequency.modecount.modecounterconfig";
    
    private int masterFileID;
    
    public static ModeCounterConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ModeCounterConfig) serializer.fromJsonFile(file, ModeCounterConfig.class);
    }
    
    public static ModeCounterConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ModeCounterConfig) serializer.fromJson(json, ModeCounterConfig.class);
    }
    
    public static ModeCounterConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ModeCounterConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, ModeCounterConfig.class);
    }
    
    public static ModeCounterConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ModeCounterConfig) serializer.fromJsonFile(fs, file, ModeCounterConfig.class);
    }
    
    public ModeCounterConfig() {
    }
    
    @JsonProperty("master_file_id")
    public void setMasterFileID(int masterFileID) {
        this.masterFileID = masterFileID;
    }
    
    @JsonProperty("master_file_id")
    public int getMasterFileID() {
        return this.masterFileID;
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
