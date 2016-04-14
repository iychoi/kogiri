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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityReducer extends Reducer<IntWritable, DoubleArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityReducer.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
        double[] accumulatedScore = null;
        
        for(DoubleArrayWritable value : values) {
            if(accumulatedScore == null) {
                accumulatedScore = new double[value.get().length];
                
                for(int i=0;i<value.get().length;i++) {
                    accumulatedScore[i] = 0;
                }
            }
            double[] darr = value.get();
            
            for(int i=0;i<darr.length;i++) {
                accumulatedScore[i] += darr[i];
            }
        }
        
        int n = (int)Math.sqrt(accumulatedScore.length);
        for(int i=0;i<accumulatedScore.length;i++) {
            int x = i/n;
            int y = i%n;
            
            String k = x + "-" + y;
            String v = Double.toString(accumulatedScore[i]);
            
            context.write(new Text(k), new Text(v));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
