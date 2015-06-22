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
package kogiri.mapreduce.readfrequency.modecount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.MultiFileIntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class ModeCounterCombiner extends Reducer<MultiFileIntWritable, CompressedIntArrayWritable, MultiFileIntWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(MultiFileIntWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        Hashtable<Integer, MutableInteger> hitsTable = new Hashtable<Integer, MutableInteger>();
        for(CompressedIntArrayWritable value : values) {
            int[] arr_value = value.get();
            if(arr_value.length < 2 && arr_value.length % 2 != 0) {
                throw new IOException("passed value is not in correct size");
            }
            
            for(int i=0;i<(arr_value.length/2);i++) {
                MutableInteger ext = hitsTable.get(arr_value[i*2]);
                if(ext == null) {
                    hitsTable.put(arr_value[i*2], new MutableInteger(arr_value[(i*2)+1]));
                } else {
                    // has
                    ext.increase(arr_value[(i*2)+1]);
                }
            }
        }
        
        List<Integer> hits = new ArrayList<Integer>();
        for(int hitsKey : hitsTable.keySet()) {
            MutableInteger hitsVal = hitsTable.get(hitsKey);
            hits.add(hitsKey);
            hits.add(hitsVal.get());
        }
        
        context.write(key, new CompressedIntArrayWritable(hits));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
    
    class MutableInteger {
        private int value;

        public MutableInteger(int value) {
            this.value = value;
        }

        public void set(int n) {
            this.value = n;
        }

        public int get() {
            return this.value;
        }

        public void increase() {
            this.value++;
        }

        public void increase(int val) {
            this.value += val;
        }
    }
}
