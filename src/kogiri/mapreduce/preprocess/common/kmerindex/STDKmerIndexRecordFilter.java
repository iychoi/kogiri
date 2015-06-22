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

import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class STDKmerIndexRecordFilter extends AKmerIndexRecordFilter {
    
    private static final Log LOG = LogFactory.getLog(STDKmerIndexRecordFilter.class);
    
    private final double avg;
    private final double stddeviation;
    private final double factor;

    public STDKmerIndexRecordFilter(double avg, double stddeviation, double factor) {
        this.avg = avg;
        this.stddeviation = stddeviation;
        this.factor = factor;
    }
    
    @Override
    public CompressedIntArrayWritable accept(CompressedSequenceWritable key, CompressedIntArrayWritable value) {
        double diffPositive = Math.abs(this.avg - value.getPositiveEntriesCount());
        double diffNegative = Math.abs(this.avg - value.getNegativeEntriesCount());
        double boundary = Math.ceil(Math.abs(this.stddeviation * this.factor));
            
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
