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
import java.math.BigInteger;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.helpers.SequenceHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerIndexRecordReader extends RecordReader<CompressedSequenceWritable, CompressedIntArrayWritable> {
    private Path[] inputIndexPaths;
    private Configuration conf;
    private AKmerIndexReader indexReader;
    private BigInteger currentProgress;
    private BigInteger progressEnd;
    private KmerIndexInputFormatConfig inputFormatConfig;
    private CompressedSequenceWritable curKey;
    private CompressedIntArrayWritable curVal;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerIndexSplit)) {
            throw new IOException("split is not an instance of KmerIndexSplit");
        }
        
        KmerIndexSplit kmerIndexSplit = (KmerIndexSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPaths = kmerIndexSplit.getIndexFilePaths();
        
        this.inputFormatConfig = new KmerIndexInputFormatConfig();
        this.inputFormatConfig.loadFrom(this.conf);
        
        FileSystem fs = this.inputIndexPaths[0].getFileSystem(this.conf);
        if(this.inputIndexPaths.length == 1) {
            this.indexReader = new SingleKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(this.inputIndexPaths)[0], this.conf);
        } else {
            this.indexReader = new ChunkedKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(this.inputIndexPaths), this.inputFormatConfig.getKmerIndexChunkInfoPath(), context, this.conf);
        }
        
        this.currentProgress = BigInteger.ZERO;
        StringBuilder endKmer = new StringBuilder();
        for(int i=0;i<this.inputFormatConfig.getKmerSize();i++) {
            endKmer.append("T");
        }
        this.progressEnd = SequenceHelper.convertToBigInteger(endKmer.toString());
        
        this.curKey = null;
        this.curVal = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        CompressedSequenceWritable key = new CompressedSequenceWritable();
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        boolean result = this.indexReader.next(key, val);
        
        if(result) {
            this.curKey = key;
            this.curVal = val;
        } else {
            this.curKey = null;
            this.curVal = null;
        }
        return result;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() throws IOException, InterruptedException {
        return this.curKey;
    }

    @Override
    public CompressedIntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return this.curVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        BigInteger divided = this.currentProgress.multiply(BigInteger.valueOf(100)).divide(this.progressEnd);
        float f = divided.floatValue() / 100;

        return Math.min(1.0f, f);
    }

    @Override
    public void close() throws IOException {
        this.indexReader.close();
    }
}
