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

import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerIndexReader extends AKmerIndexReader {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexReader.class);
    
    private static final int BUFFER_SIZE = 100;
    
    private FileSystem fs;
    private Path indexIndexPath;
    private Path[] indexPaths;
    private Configuration conf;
    private TaskAttemptContext context;
    private IndexCloseableMapFileReader[] mapfileReaders;
    private CompressedSequenceWritable beginKey;
    private CompressedSequenceWritable endKey;
    private BlockingQueue<KmerIndexBufferEntry> buffer = new LinkedBlockingQueue<KmerIndexBufferEntry>();
    private String[] chunkLastKeys;
    private boolean eof;
    
    private int currentIndex;
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, null, null, null, context, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, Path[] kmerIndexPartPath, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, kmerIndexPartPath, null, null, context, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, null, beginKey, endKey, context, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, Path[] kmerIndexPartPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, kmerIndexPartPath, beginKey, endKey, context, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, String beginKey, String endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, null, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), context, conf);
    }
    
    public KmerIndexReader(FileSystem fs, Path kmerIndexIndexPath, Path[] kmerIndexPartPath, String beginKey, String endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, kmerIndexIndexPath, kmerIndexPartPath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), context, conf);
    }
    
    private Path[] reorderIndexParts(Path[] indexPaths) {
        int len = indexPaths.length;
        Path[] orderedArr = new Path[len];
        
        for(int i=0;i<len;i++) {
            int part = KmerIndexHelper.getIndexPartID(indexPaths[i]);
            if(part < 0 || part >= len) {
                return null;
            } else {
                orderedArr[part] = indexPaths[i];
            }
        }
        
        for(int i=0;i<len;i++) {
            if(orderedArr[i] == null) {
                return null;
            }
        }
        
        return orderedArr;
    }
    
    private void initialize(FileSystem fs, Path kmerIndexIndexPath, Path[] kmerIndexPartPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        this.fs = fs;
        this.context = context;
        this.conf = conf;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.indexIndexPath = kmerIndexIndexPath;

        Path[] kmerIndexPartFilePath = null;
        if(kmerIndexPartPath == null) {
            kmerIndexPartFilePath = KmerIndexHelper.getKmerIndexPartFilePath(conf, this.indexIndexPath);
        } else {
            kmerIndexPartFilePath = kmerIndexPartPath;
        }
        
        this.indexPaths = reorderIndexParts(kmerIndexPartFilePath);
        if(this.indexPaths == null) {
            throw new IOException("part of index is missing");
        }
        
        this.mapfileReaders = new IndexCloseableMapFileReader[this.indexPaths.length];

        KmerIndexIndex indexIndex = KmerIndexIndex.createInstance(fs, this.indexIndexPath);
        
        this.chunkLastKeys = indexIndex.getSortedLastKeys().toArray(new String[0]);
        
        if(this.chunkLastKeys.length != this.indexPaths.length) {
            throw new IOException("KmerIndexChunkKeys length is different from given index group length");
        }
        
        this.currentIndex = 0;
        if(beginKey != null) {
            boolean bFound = false;
            for(int i=0;i<this.chunkLastKeys.length;i++) {
                if(this.chunkLastKeys[i].compareToIgnoreCase(beginKey.getSequence()) >= 0) {
                    //found
                    this.currentIndex = i;
                    bFound = true;
                    break;
                }
            }
            
            if(!bFound) {
                throw new IOException("Could not find start point from kmer index");
            }
        }
        
        this.mapfileReaders[this.currentIndex] = new IndexCloseableMapFileReader(fs, this.indexPaths[this.currentIndex].toString(), conf);
        if(beginKey != null) {
            this.eof = false;
            seek(beginKey);
        } else {
            this.eof = false;
            fillBuffer();
        }
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            CompressedSequenceWritable lastBufferedKey = null;
            int added = 0;
            while(added < BUFFER_SIZE) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.mapfileReaders[this.currentIndex].next(key, val)) {
                    KmerIndexBufferEntry entry = new KmerIndexBufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                    
                    lastBufferedKey = key;
                    added++;
                } else {
                    // EOF of this part
                    this.mapfileReaders[this.currentIndex].close();
                    this.mapfileReaders[this.currentIndex] = null;
                    this.currentIndex++;
                    
                    if(this.currentIndex == this.mapfileReaders.length) {
                        // last
                        this.eof = true;
                        break;
                    } else {
                        this.mapfileReaders[this.currentIndex] = new IndexCloseableMapFileReader(this.fs, this.indexPaths[this.currentIndex].toString(), this.conf);
                        this.mapfileReaders[this.currentIndex].closeIndex();
                    }
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
        if(this.mapfileReaders != null) {
            for(int i=0;i<this.mapfileReaders.length;i++) {
                if(this.mapfileReaders[i] != null) {
                    this.mapfileReaders[i].close();
                    this.mapfileReaders[i] = null;
                }
            }
            this.mapfileReaders = null;
        }
    }
    
    @Override
    public Path getIndexPath() {
        return this.indexIndexPath;
    }
    
    private void seek(String sequence) throws IOException {
        seek(new CompressedSequenceWritable(sequence));
    }
    
    private void seek(CompressedSequenceWritable key) throws IOException {
        this.buffer.clear();
        
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        CompressedSequenceWritable nextKey = (CompressedSequenceWritable)this.mapfileReaders[this.currentIndex].getClosest(key, val);
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
