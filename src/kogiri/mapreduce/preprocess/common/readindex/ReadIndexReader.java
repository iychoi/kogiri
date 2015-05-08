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
package kogiri.mapreduce.preprocess.common.readindex;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class ReadIndexReader implements java.io.Closeable {
    
    private static final Log LOG = LogFactory.getLog(ReadIndexReader.class);
    
    private FileSystem fs;
    private String indexPath;
    private Configuration conf;
    private MapFile.Reader mapfileReader;
    
    public ReadIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        this.fs = fs;
        this.indexPath = indexPath;
        this.conf = conf;
        
        // create a new AugMapFile reader
        this.mapfileReader = new MapFile.Reader(fs, indexPath, conf);
    }
    
    public String getIndexPath() {
        return this.indexPath;
    }
    
    public void reset() throws IOException {
        this.mapfileReader.reset();
    }
    
    public boolean next(LongWritable key, IntWritable val) throws IOException {
        return this.mapfileReader.next(key, val);
    }
    
    public int findReadID(long offset) throws ReadIDNotFoundException {
        LongWritable key = new LongWritable(offset);
        IntWritable value = new IntWritable();
        try {
            this.mapfileReader.get(key, value);
            return value.get();
        } catch (IOException ex) {
            LOG.error(ex);
        }

        throw new ReadIDNotFoundException("ReadID is not found");
    }

    @Override
    public void close() throws IOException {
        this.mapfileReader.close();
    }
}
