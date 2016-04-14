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
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.mapreduce.preprocess.common.kmerstatistics.KmerStandardDeviation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class STDKmerIndexRecordFilter extends AKmerIndexRecordFilter {
    
    private static final Log LOG = LogFactory.getLog(STDKmerIndexRecordFilter.class);

    private KmerStandardDeviation stddev;
    
    public STDKmerIndexRecordFilter(Configuration conf) throws IOException {
        this.stddev = KmerStandardDeviation.createInstance(conf);
    }
    
    public STDKmerIndexRecordFilter(KmerStandardDeviation stddev) {
        this.stddev = stddev;
    }
    
    @Override
    public CompressedIntArrayWritable accept(CompressedSequenceWritable key, CompressedIntArrayWritable value) {
        if(this.stddev.getFactor() <= 0) {
            return new CompressedIntArrayWritable(value);
        }
        
        double diffPositive = Math.abs(this.stddev.getAverage() - value.getPositiveEntriesCount());
        double diffNegative = Math.abs(this.stddev.getAverage() - value.getNegativeEntriesCount());
        double boundary = Math.ceil(Math.abs(this.stddev.getStdDeviation() * this.stddev.getFactor()));
            
        if(diffPositive <= boundary && diffNegative <= boundary) {
            return new CompressedIntArrayWritable(value);
        } else if(diffPositive <= boundary && value.getPositiveEntriesCount() > 0) {
            return new CompressedIntArrayWritable(value.getPositiveEntries());
        } else if(diffNegative <= boundary && value.getNegativeEntriesCount() > 0) {
            return new CompressedIntArrayWritable(value.getNegativeEntries());
        } else {
            return new CompressedIntArrayWritable();
        }
    }
}
