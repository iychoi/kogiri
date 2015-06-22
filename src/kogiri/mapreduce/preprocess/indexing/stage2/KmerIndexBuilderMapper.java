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
import kogiri.common.algorithms.KmerKeySelection.KmerRecord;
import kogiri.common.fasta.FastaRead;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.helpers.ReadIndexHelper;
import kogiri.mapreduce.preprocess.common.readindex.ReadIDNotFoundException;
import kogiri.mapreduce.preprocess.common.readindex.ReadIndexReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderMapper extends Mapper<LongWritable, FastaRead, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderMapper.class);
    
    private PreprocessorConfig ppConfig;
    
    private int previousReadID;
    private ReadIndexReader readIndexReader;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorConfig.createInstance(conf);
        
        this.previousReadID = -1;
        
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        Path inputFilePath = inputSplit.getPath();
        
        Path readIndexPath = new Path(this.ppConfig.getReadIndexPath(), ReadIndexHelper.makeReadIndexFileName(inputFilePath.getName()));
        FileSystem fs = readIndexPath.getFileSystem(conf);
        if(fs.exists(readIndexPath)) {
            this.readIndexReader = new ReadIndexReader(fs, readIndexPath.toString(), conf);
        } else {
            throw new IOException("ReadIDIndex is not found");
        }
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        int readID = 0;
        if(value.getContinuousRead()) {
            readID = this.previousReadID + 1;
        } else {
            long startOffset = value.getReadOffset();
            try {
                readID = this.readIndexReader.findReadID(startOffset);
            } catch (ReadIDNotFoundException ex) {
                throw new IOException("No Read ID found : offset " + startOffset);
            }
        }
        
        this.previousReadID = readID;
        
        String sequence = value.getSequence();
        
        for (int i = 0; i < (sequence.length() - this.ppConfig.getKmerSize() + 1); i++) {
            String kmer = sequence.substring(i, i + this.ppConfig.getKmerSize());
            
            KmerRecord kmerRecord = new KmerRecord(kmer);
            KmerRecord keyRecord = kmerRecord.getSelectedKey();
            
            int[] rid_arr = new int[1];
            if(keyRecord.isReverseComplement()) {
                rid_arr[0] = -readID;
            } else {
                rid_arr[0] = readID;
            }
            
            context.write(new CompressedSequenceWritable(keyRecord.getSequence()), new CompressedIntArrayWritable(rid_arr));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.readIndexReader.close();
        this.readIndexReader = null;
    }
}
