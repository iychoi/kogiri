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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class KmerIndexSplit extends InputSplit implements Writable {

    private Path[] indexPaths;
    private String[] locations;

    public KmerIndexSplit() {    
    }
    
    public KmerIndexSplit(Path[] indexFilePaths, Configuration conf) throws IOException {
        this.indexPaths = indexFilePaths;
        this.locations = findBestLocations(indexFilePaths, conf);
    }
    
    private String[] findBestLocations(Path[] indexFilePaths, Configuration conf) throws IOException {
        Hashtable<String, MutableInteger> blkLocations = new Hashtable<String, MutableInteger>();
        for(Path path : indexFilePaths) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus fileStatus = fs.getFileStatus(path);
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for(BlockLocation location : fileBlockLocations) {
                for(String host : location.getHosts()) {
                    MutableInteger cnt = blkLocations.get(host);
                    if(cnt == null) {
                        blkLocations.put(host, new MutableInteger(1));
                    } else {
                        cnt.increase();
                    }
                }
            }
        }
        
        List<String> blkLocationsArr = new ArrayList<String>();
        for(String key : blkLocations.keySet()) {
            blkLocationsArr.add(key);
        }
        
        if(blkLocationsArr.size() == 0) {
            return new String[] {"localhost"};
        } else {
            return blkLocationsArr.toArray(new String[0]);
        }
    }
    
    public Path[] getIndexFilePaths() {
        return this.indexPaths;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Path path : this.indexPaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(path.toString());
        }
        return sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.indexPaths.length);
        for (Path indexPath : this.indexPaths) {
            Text.writeString(out, indexPath.toString());
        }
        
        out.writeInt(this.locations.length);
        for (String host : this.locations) {
            Text.writeString(out, host);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.indexPaths = new Path[in.readInt()];
        for(int i=0;i<this.indexPaths.length;i++) {
            this.indexPaths[i] = new Path(Text.readString(in));
        }
        
        this.locations = new String[in.readInt()];
        for(int i=0;i<this.locations.length;i++) {
            this.locations[i] = Text.readString(in);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }
    
    class MutableInteger {

        private int value;

        public MutableInteger(int value) {
            this.value = value;
        }

        public void set(int n) {
            this.value = n;
        }

        public int get() {
            return this.value;
        }

        public void increase() {
            this.value++;
        }

        public void increase(int val) {
            this.value += val;
        }
    }
}
