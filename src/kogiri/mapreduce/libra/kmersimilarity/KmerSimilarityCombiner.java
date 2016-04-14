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
package kogiri.mapreduce.libra.kmersimilarity;

import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.DoubleArrayWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityCombiner extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
        double[] accumulatedDiff = null;
        
        for(DoubleArrayWritable value : values) {
            if(accumulatedDiff == null) {
                accumulatedDiff = new double[value.get().length];
                
                for(int i=0;i<value.get().length;i++) {
                    accumulatedDiff[i] = 0;
                }
            }
            double[] darr = value.get();
            
            for(int i=0;i<darr.length;i++) {
                accumulatedDiff[i] += darr[i];
            }
        }
        
        context.write(key, new DoubleArrayWritable(accumulatedDiff));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}
