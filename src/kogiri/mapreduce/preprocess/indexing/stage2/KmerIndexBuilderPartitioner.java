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
package kogiri.mapreduce.preprocess.indexing.stage2;


import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.hadoop.io.datatypes.MultiFileCompressedSequenceWritable;
import kogiri.common.helpers.SequenceHelper;
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.helpers.KmerHistogramHelper;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerHistogram;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerRangePartition;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerRangePartitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderPartitioner extends Partitioner<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable> implements Configurable {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    private Configuration conf;
    
    private boolean initialized = false;
    private PreprocessorConfig ppConfig;
    private NamedOutputs namedOutputs;
    private KmerRangePartition[][] partitions;
    private CompressedSequenceWritable[][] partitionEndKeys;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
    private void initialize() throws IOException {
        this.ppConfig = PreprocessorConfig.createInstance(this.conf);
        this.namedOutputs = NamedOutputs.createInstance(this.conf);
        
        this.partitions = new KmerRangePartition[this.namedOutputs.getSize()][];
        this.partitionEndKeys = new CompressedSequenceWritable[this.namedOutputs.getSize()][];
    }
    
    private void initialize(int fileID, int numReduceTasks) throws IOException {
        if(this.partitionEndKeys[fileID] == null) {
            KmerHistogram histogram = null;
            // search index file
            String filename = this.namedOutputs.getRecordFromID(fileID).getFilename();
            Path histogramHadoopPath = new Path(this.ppConfig.getKmerHistogramPath(), KmerHistogramHelper.makeKmerHistogramFileName(filename));
            FileSystem fs = histogramHadoopPath.getFileSystem(this.conf);
            if (fs.exists(histogramHadoopPath)) {
                histogram = KmerHistogram.createInstance(histogramHadoopPath, fs);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }

            KmerRangePartitioner partitioner = new KmerRangePartitioner(this.ppConfig.getKmerSize(), numReduceTasks);
            this.partitions[fileID] = partitioner.getHistogramPartitions(histogram.getSortedRecord(), histogram.getTotalSampleCount());

            this.partitionEndKeys[fileID] = new CompressedSequenceWritable[numReduceTasks];
            for (int i = 0; i < this.partitions[fileID].length; i++) {
                try {
                    this.partitionEndKeys[fileID][i] = new CompressedSequenceWritable(this.partitions[fileID][i].getPartitionEndKmer());
                } catch (IOException ex) {
                    throw new RuntimeException(ex.toString());
                }
            }
        }
    }
    
    @Override
    public int getPartition(MultiFileCompressedSequenceWritable key, CompressedIntArrayWritable value, int numReduceTasks) {
        if(!this.initialized) {
            try {
                initialize();
                this.initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException(ex.toString());
            }
        }
        
        try {
            initialize(key.getFileID(), numReduceTasks);
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }
        
        int partition = getPartitionIndex(key);
        if(partition < 0) {
            throw new RuntimeException("partition failed");
        }
        
        return partition;
    }

    private int getPartitionIndex(MultiFileCompressedSequenceWritable key) {
        int fileID = key.getFileID();
        for(int i=0;i<this.partitionEndKeys[fileID].length;i++) {
            int comp = SequenceHelper.compareSequences(key.getCompressedSequence(), this.partitionEndKeys[fileID][i].getCompressedSequence());
            if(comp <= 0) {
                return i;
            }
        }
        return -1;
    }
}
