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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.hadoop.io.reader.map.IndexCloseableMapFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author iychoi
 */
public class SingleKmerIndexReader extends AKmerIndexReader {
    
    private static final Log LOG = LogFactory.getLog(SingleKmerIndexReader.class);
    
    private static final int BUFFER_SIZE = 1000;
    
    private FileSystem fs;
    private String indexPath;
    private Configuration conf;
    private IndexCloseableMapFileReader mapfileReader;
    private CompressedSequenceWritable beginKey;
    private CompressedSequenceWritable endKey;
    private BlockingQueue<KmerIndexBufferEntry> buffer = new LinkedBlockingQueue<KmerIndexBufferEntry>();
    private boolean eof;
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, null, null, conf);
    }
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        initialize(fs, indexPath, beginKey, endKey, conf);
    }
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, String beginKey, String endKey, Configuration conf) throws IOException {
        initialize(fs, indexPath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf) throws IOException {
        this.fs = fs;
        this.indexPath = indexPath;
        this.conf = conf;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.mapfileReader = new IndexCloseableMapFileReader(fs, indexPath, conf);
        if(beginKey != null) {
            seek(beginKey);
        } else {
            this.eof = false;
            fillBuffer();
        }
        this.mapfileReader.closeIndex();
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            CompressedSequenceWritable lastBufferedKey = null;
            for(int i=0;i<BUFFER_SIZE;i++) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.mapfileReader.next(key, val)) {
                    KmerIndexBufferEntry entry = new KmerIndexBufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                    
                    lastBufferedKey = key;
                } else {
                    // EOF
                    this.eof = true;
                    break;
                }
            }
            
            if(this.endKey != null && lastBufferedKey != null) {
                if(lastBufferedKey.compareTo(this.endKey) > 0) {
                    // recheck buffer
                    BlockingQueue<KmerIndexBufferEntry> new_buffer = new LinkedBlockingQueue<KmerIndexBufferEntry>();

                    KmerIndexBufferEntry entry = this.buffer.poll();
                    while(entry != null) {
                        if(entry.getKey().compareTo(this.endKey) <= 0) {
                            if(!new_buffer.offer(entry)) {
                                throw new IOException("buffer is full");
                            }
                        }

                        entry = this.buffer.poll();
                    }

                    this.buffer = new_buffer;
                    this.eof = true;
                }
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        if(this.mapfileReader != null) {
            this.mapfileReader.close();
            this.mapfileReader = null;
        }
        
        if(this.buffer != null) {
            this.buffer.clear();
            this.buffer = null;
        }
    }
    
    @Override
    public String[] getIndexPaths() {
        return new String[] {this.indexPath};
    }
    
    private void seek(String sequence) throws IOException {
        seek(new CompressedSequenceWritable(sequence));
    }
    
    private void seek(CompressedSequenceWritable key) throws IOException {
        this.buffer.clear();
        
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        CompressedSequenceWritable nextKey = (CompressedSequenceWritable)this.mapfileReader.getClosest(key, val);
        if(nextKey == null) {
            this.eof = true;
        } else {
            this.eof = false;
            
            if(this.endKey != null) {
                if(nextKey.compareTo(this.endKey) <= 0) {
                    KmerIndexBufferEntry entry = new KmerIndexBufferEntry(nextKey, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }

                    fillBuffer();
                } else {
                    this.eof = true;
                }
            } else {
                KmerIndexBufferEntry entry = new KmerIndexBufferEntry(nextKey, val);
                if(!this.buffer.offer(entry)) {
                    throw new IOException("buffer is full");
                }

                fillBuffer();
            }
        }
    }
    
    @Override
    public boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException {
        KmerIndexBufferEntry entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal());
            return true;
        }
        
        fillBuffer();
        entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal());
            return true;
        }
        return false;
    }
}