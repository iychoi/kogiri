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
import kogiri.common.helpers.SequenceHelper;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
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
public class KmerIndexBuilderPartitioner extends Partitioner<CompressedSequenceWritable, CompressedIntArrayWritable> implements Configurable {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    private final static String HISTOGRAM_FILE = "kogiri.mapreduce.kmerindex.histogram_file";
    
    private Configuration conf;
    
    private boolean initialized = false;
    private PreprocessorConfig ppConfig;
    private KmerRangePartition[] partitions;
    private CompressedSequenceWritable[] partitionEndKeys;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
    public static void setHistogramPath(Configuration config, Path histogramPath) throws IOException {
        config.set(HISTOGRAM_FILE, histogramPath.toString());
    }
    
    public static Path getHistogramPath(Configuration config) throws IOException {
        String histogramPath = config.get(HISTOGRAM_FILE);
        if(histogramPath == null) {
            return null;
        }
        return new Path(histogramPath);
    }
    
    private void initialize() throws IOException {
        this.ppConfig = PreprocessorConfig.createInstance(this.conf);
        
        this.partitions = null;
        this.partitionEndKeys = null;
    }
    
    private void initializeSecond(int numReduceTasks) throws IOException {
        if(this.partitions == null) {
            KmerHistogram histogram = null;
            // search index file
            Path histogramPath = getHistogramPath(this.conf);
            FileSystem fs = histogramPath.getFileSystem(this.conf);
            if (fs.exists(histogramPath)) {
                histogram = KmerHistogram.createInstance(fs, histogramPath);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }

            KmerRangePartitioner partitioner = new KmerRangePartitioner(this.ppConfig.getKmerSize(), numReduceTasks);
            this.partitions = partitioner.getHistogramPartitions(histogram.getSortedRecord(), histogram.getTotalKmerCount());

            this.partitionEndKeys = new CompressedSequenceWritable[this.partitions.length];
            for (int i = 0; i < this.partitions.length; i++) {
                try {
                    this.partitionEndKeys[i] = new CompressedSequenceWritable(this.partitions[i].getPartitionEndKmer());
                } catch (IOException ex) {
                    throw new RuntimeException(ex.toString());
                }
            }
        }
    }
    
    @Override
    public int getPartition(CompressedSequenceWritable key, CompressedIntArrayWritable value, int numReduceTasks) {
        if(!this.initialized) {
            try {
                initialize();
                this.initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException(ex.toString());
            }
        }
        
        try {
            initializeSecond(numReduceTasks);
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }
        
        int partition = getPartitionIndex(key);
        if(partition < 0) {
            throw new RuntimeException("partition failed");
        }
        
        return partition;
    }

    private int getPartitionIndex(CompressedSequenceWritable key) {
        for(int i=0;i<this.partitionEndKeys.length;i++) {
            int comp = SequenceHelper.compareSequences(key.getCompressedSequence(), this.partitionEndKeys[i].getCompressedSequence());
            if(comp <= 0) {
                return i;
            }
        }
        return -1;
    }
}
