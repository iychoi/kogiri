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
import java.util.ArrayList;
import java.util.List;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.hadoop.io.datatypes.MultiFileCompressedSequenceWritable;
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderReducer extends Reducer<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderReducer.class);
    
    private NamedOutputs namedOutputs;
    private MultipleOutputs mos;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = NamedOutputs.createInstance(conf);
        
        this.mos = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(MultiFileCompressedSequenceWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        List<Integer> readIDs = new ArrayList<Integer>();
        
        for(CompressedIntArrayWritable value : values) {
            for(int ivalue : value.get()) {
                readIDs.add(ivalue);
            }
        }
        
        int namedoutputID = key.getFileID();
        String namedOutput = this.namedOutputs.getRecordFromID(namedoutputID).getIdentifier();
        
        CompressedSequenceWritable outKey = new CompressedSequenceWritable(key.getCompressedSequence(), key.getSequenceLength());
        
        if(this.mos != null) {
            this.mos.write(namedOutput, outKey, new CompressedIntArrayWritable(readIDs));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
    
        if(this.mos != null) {
            this.mos.close();
        }
    }
}
