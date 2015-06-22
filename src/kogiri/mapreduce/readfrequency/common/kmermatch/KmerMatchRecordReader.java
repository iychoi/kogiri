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
package kogiri.mapreduce.readfrequency.common.kmermatch;

import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerRangePartition;
import kogiri.mapreduce.preprocess.common.kmerindex.AKmerIndexRecordFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerMatchRecordReader extends RecordReader<CompressedSequenceWritable, KmerMatchResult> {
    
    private static final Log LOG = LogFactory.getLog(KmerMatchRecordReader.class);
    
    private Path[] inputIndexPath;
    private KmerJoiner matcher;
    private Configuration conf;
    private KmerMatchResult curResult;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerMatchInputSplit)) {
            throw new IOException("split is not an instance of KmerMatchIndexSplit");
        }
        
        KmerMatchInputSplit kmerIndexSplit = (KmerMatchInputSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPath = kmerIndexSplit.getIndexFilePath();
        
        KmerRangePartition partition = kmerIndexSplit.getPartition();
        
        KmerMatchInputFormatConfig inputFormatConfig = KmerMatchInputFormatConfig.createInstance(this.conf);
        Class kmerIndexRecordFilterClazz = inputFormatConfig.getKmerIndexRecordFilterClass();
        AKmerIndexRecordFilter kmerIndexRecordFilter = null;
        if(kmerIndexRecordFilterClazz != null) {
            try {
                kmerIndexRecordFilter = (AKmerIndexRecordFilter) kmerIndexRecordFilterClazz.newInstance();
            } catch (InstantiationException ex) {
                LOG.error(ex);
            } catch (IllegalAccessException ex) {
                LOG.error(ex);
            }
        }
        
        this.matcher = new KmerJoiner(this.inputIndexPath, partition, kmerIndexRecordFilter, context);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.curResult = this.matcher.stepNext();
        if(this.curResult != null) {
            return true;
        }
        return false;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() {
        if(this.curResult != null) {
            return this.curResult.getKey();
        }
        return null;
    }

    @Override
    public KmerMatchResult getCurrentValue() {
        return this.curResult;
    }

    @Override
    public float getProgress() throws IOException {
        return this.matcher.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.matcher.close();
    }
}
