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
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class ModeCounterReducer extends Reducer<MultiFileIntWritable, CompressedIntArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterReducer.class);
    
    private NamedOutputs namedOutputs = null;
    private MultipleOutputs mos = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = NamedOutputs.createInstance(conf);
        
        this.mos = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(MultiFileIntWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int namedoutputID = key.getFileID();
        String namedOutput = this.namedOutputs.getRecordFromID(namedoutputID).getIdentifier();
                
        List<CompressedIntArrayWritable> forward_list = new ArrayList<CompressedIntArrayWritable>();
        List<CompressedIntArrayWritable> reverse_list = new ArrayList<CompressedIntArrayWritable>();
        for(CompressedIntArrayWritable value : values) {
            int[] arr_value = value.get();
            if(arr_value.length < 2 && arr_value.length % 2 != 0) {
                throw new IOException("passed value is not in correct size");
            }
            
            for(int i=0;i<(arr_value.length / 2);i++) {
                int hit =  arr_value[i*2];
                if(hit > 0) {
                    int[] entry_val = new int[2];
                    entry_val[0] = arr_value[i*2];
                    entry_val[1] = arr_value[(i*2) + 1];
                    CompressedIntArrayWritable entry = new CompressedIntArrayWritable(entry_val);

                    forward_list.add(entry);
                } else if(hit < 0) {
                    int[] entry_val = new int[2];
                    entry_val[0] = Math.abs(arr_value[i*2]);
                    entry_val[1] = arr_value[(i*2) + 1];
                    CompressedIntArrayWritable entry = new CompressedIntArrayWritable(entry_val);
                    
                    reverse_list.add(entry);
                }
            }
        }
        
        int[] forward_mode = getMode(forward_list);
        int[] reverse_mode = getMode(reverse_list);
        
        // pick max count
        int larger_mode = 0;
        if(forward_mode[1] >= reverse_mode[1]) {
            larger_mode = forward_mode[0];
        } else {
            larger_mode = reverse_mode[0];
        }
        
        String out_value = String.valueOf(larger_mode);
        
        if(this.mos != null) {
            this.mos.write(namedOutput, new Text(String.valueOf(key.getValue())), new Text(out_value));
        }
    }
    
    private int[] getMode(List<CompressedIntArrayWritable> values) throws IOException {
        Hashtable<Integer, MutableInteger> modeTable = new Hashtable<Integer, MutableInteger>();
        
        int mode_hit = 0;
        int mode_count = 0;
        for(CompressedIntArrayWritable value : values) {
            int[] arrval = value.get();
            int hit = arrval[0];
            int cnt = arrval[1];
            
            if(hit == 0) {
                continue;
            }

            MutableInteger cntExist = modeTable.get(hit);
            if(cntExist == null) {
                MutableInteger new_cnt = new MutableInteger(cnt);
                modeTable.put(hit, new_cnt);
                if(mode_hit == 0) {
                    mode_hit = hit;
                    mode_count = new_cnt.get();
                } else {
                    if(mode_count < new_cnt.get()) {
                        mode_hit = hit;
                        mode_count = new_cnt.get();
                    } else if(mode_count == new_cnt.get() && mode_hit < hit) {
                        mode_hit = hit;
                        mode_count = new_cnt.get();
                    }
                }
            } else {
                // existing
                cntExist.increase(cnt);
                
                if(hit == mode_hit) {
                    mode_count = cntExist.get();
                } else {
                    if(mode_count < cntExist.get()) {
                        mode_hit = hit;
                        mode_count = cntExist.get();
                    } else if(mode_count == cntExist.get() && mode_hit < hit) {
                        mode_hit = hit;
                        mode_count = cntExist.get();
                    }
                }
            }
        }
        
        modeTable.clear();
        
        int[] mode = new int[2];
        mode[0] = mode_hit;
        mode[1] = mode_count;
        
        return mode;
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
        
        if(this.mos != null) {
            this.mos.close();
        }
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
