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
package kogiri.mapreduce.common.kmermatch;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.helpers.SequenceHelper;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerRangePartition;
import kogiri.mapreduce.preprocess.common.kmerindex.AKmerIndexReader;
import kogiri.mapreduce.preprocess.common.kmerindex.AKmerIndexRecordFilter;
import kogiri.mapreduce.preprocess.common.kmerindex.FilteredKmerIndexReader;
import kogiri.mapreduce.preprocess.common.kmerindex.KmerIndexReader;
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
public class KmerJoiner {
    
    private static final Log LOG = LogFactory.getLog(KmerJoiner.class);
    
    private Path[] kmerIndexPath;
    private KmerRangePartition partition;
    private AKmerIndexRecordFilter[] recordFilter;
    private Configuration conf;
    
    private AKmerIndexReader[] readers;
    private BigInteger partitionSize;
    private CompressedSequenceWritable progressKey;
    private boolean eof;
    private BigInteger beginSequence;
    
    private CompressedSequenceWritable[] stepKeys;
    private CompressedIntArrayWritable[] stepVals;
    private List<Integer> stepMinKeys;
    private boolean stepStarted;
    
    public KmerJoiner(Path[] kmerIndexPath, KmerRangePartition partition, AKmerIndexRecordFilter[] filter, TaskAttemptContext context) throws IOException {
        initialize(kmerIndexPath, partition, filter, context.getConfiguration());
    }
    
    public KmerJoiner(Path[] kmerIndexPath, KmerRangePartition partition, AKmerIndexRecordFilter[] filter, Configuration conf) throws IOException {
        initialize(kmerIndexPath, partition, filter, conf);
    }
    
    private void initialize(Path[] kmerIndexPath, KmerRangePartition partition, AKmerIndexRecordFilter[] filter, Configuration conf) throws IOException {
        this.kmerIndexPath = kmerIndexPath;
        this.partition = partition;
        this.recordFilter = filter;
        this.conf = conf;
        
        this.readers = new AKmerIndexReader[this.kmerIndexPath.length];
        LOG.info("# of KmerIndexReader : " + this.readers.length);
        for(int i=0;i<this.readers.length;i++) {
            FileSystem fs = this.kmerIndexPath[i].getFileSystem(this.conf);
            
            if(this.recordFilter == null) {
                this.readers[i] = new KmerIndexReader(fs, this.kmerIndexPath[i], this.partition.getPartitionBeginKmer(), this.partition.getPartitionEndKmer(), this.conf);
            } else {
                if(this.recordFilter.length != this.readers.length) {
                    this.readers[i] = new FilteredKmerIndexReader(fs, this.kmerIndexPath[i], this.partition.getPartitionBeginKmer(), this.partition.getPartitionEndKmer(), this.recordFilter[0], this.conf);
                } else {
                    this.readers[i] = new FilteredKmerIndexReader(fs, this.kmerIndexPath[i], this.partition.getPartitionBeginKmer(), this.partition.getPartitionEndKmer(), this.recordFilter[i], this.conf);
                }
            }
        }
        
        this.partitionSize = partition.getPartitionSize();
        this.progressKey = null;
        this.eof = false;
        this.beginSequence = this.partition.getPartitionBegin();
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        
        LOG.info("Matcher is initialized");
        LOG.info("> Range " + this.partition.getPartitionBeginKmer() + " ~ " + this.partition.getPartitionEndKmer());
        LOG.info("> Num of Slice Entries : " + this.partition.getPartitionSize().longValue());
    }
    
    public KmerMatchResult stepNext() throws IOException {
        List<Integer> minKeyIndice = getNextMinKeys();
        if(minKeyIndice.size() > 0) {
            CompressedSequenceWritable minKey = this.stepKeys[minKeyIndice.get(0)];
            this.progressKey = minKey;
            
            // check matching
            CompressedIntArrayWritable[] minVals = new CompressedIntArrayWritable[minKeyIndice.size()];
            Path[] minIndexPaths = new Path[minKeyIndice.size()];

            int valIdx = 0;
            for (int idx : minKeyIndice) {
                minVals[valIdx] = this.stepVals[idx];
                minIndexPaths[valIdx] = this.readers[idx].getIndexPath();
                valIdx++;
            }

            return new KmerMatchResult(minKey, minVals, minIndexPaths);
        }
        
        // step failed and no match
        this.eof = true;
        this.progressKey = null;
        return null;
    }
    
    private List<Integer> findMinKeys() throws IOException {
        CompressedSequenceWritable minKey = null;
        List<Integer> minKeyIndice = new ArrayList<Integer>();
        for(int i=0;i<this.readers.length;i++) {
            if(this.stepKeys[i] != null) {
                if(minKey == null) {
                    minKey = this.stepKeys[i];
                    minKeyIndice.clear();
                    minKeyIndice.add(i);
                } else {
                    int comp = minKey.compareTo(this.stepKeys[i]);
                    if (comp == 0) {
                        // found same min key
                        minKeyIndice.add(i);
                    } else if (comp > 0) {
                        // found smaller one
                        minKey = this.stepKeys[i];
                        minKeyIndice.clear();
                        minKeyIndice.add(i);
                    }
                }
            }
        }

        return minKeyIndice;
    }
    
    private List<Integer> getNextMinKeys() throws IOException {
        if(!this.stepStarted) {
            for(int i=0;i<this.readers.length;i++) {
                // fill first
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[i].next(key, val)) {
                    this.stepKeys[i] = key;
                    this.stepVals[i] = val;
                } else {
                    this.stepKeys[i] = null;
                    this.stepVals[i] = null;
                }
            }
            
            this.stepStarted = true;
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        } else {
            // find min key
            if(this.stepMinKeys.size() == 0) {
                //EOF
                return this.stepMinKeys;
            }
            
            // move min pointers
            for (int idx : this.stepMinKeys) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[idx].next(key, val)) {
                    this.stepKeys[idx] = key;
                    this.stepVals[idx] = val;
                } else {
                    this.stepKeys[idx] = null;
                    this.stepVals[idx] = null;
                }
            }
            
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        }
    }
    
    public float getProgress() {
        if(this.progressKey == null) {
            if(this.eof) {
                return 1.0f;
            } else {
                return 0.0f;
            }
        } else {
            BigInteger seq = SequenceHelper.convertToBigInteger(this.progressKey.getSequence());
            BigInteger prog = seq.subtract(this.beginSequence);
            int comp = this.partitionSize.compareTo(prog);
            if (comp <= 0) {
                return 1.0f;
            } else {
                BigDecimal progDecimal = new BigDecimal(prog);
                BigDecimal rate = progDecimal.divide(new BigDecimal(this.partitionSize), 3, BigDecimal.ROUND_HALF_UP);
                
                float f = rate.floatValue();
                return Math.min(1.0f, f);
            }
        }
    }
    
    public void close() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
