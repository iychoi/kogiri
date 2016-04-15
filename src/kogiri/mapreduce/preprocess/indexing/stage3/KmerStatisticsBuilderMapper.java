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
package kogiri.mapreduce.preprocess.indexing.stage3;

import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.preprocess.common.helpers.KmerStatisticsHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilderMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilderMapper.class);
    
    private Counter uniqueKmerCounter;
    private Counter totalKmerCounter;
    private Counter squareKmerCounter;
    private Counter logTFSquareCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.uniqueKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameUnique(), fastaFileName);
        this.totalKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameTotal(), fastaFileName);
        this.squareKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameSquare(), fastaFileName);
        this.logTFSquareCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameLogTFSquare(), fastaFileName);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        int pos = value.getPositiveEntriesCount();
        if(pos > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(pos);
            this.squareKmerCounter.increment((long) Math.pow(pos, 2));
            //this.logTFSquareCounter.increment((long) (Math.pow(1 + Math.log10(pos), 2) * 1000));
        }
        
        int neg = value.getNegativeEntriesCount();
        if(neg > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(neg);
            this.squareKmerCounter.increment((long) Math.pow(neg, 2));
            //this.logTFSquareCounter.increment((long) (Math.pow(1 + Math.log10(neg), 2) * 1000));
        }
        
        int sum = pos + neg;
        if(sum > 0) {
            this.logTFSquareCounter.increment((long) (Math.pow(1 + Math.log10(sum), 2) * 1000));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
