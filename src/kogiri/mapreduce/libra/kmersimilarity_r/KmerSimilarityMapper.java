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
package kogiri.mapreduce.libra.kmersimilarity_r;

import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.mapreduce.common.kmermatch.KmerMatchFileMapping;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private KmerMatchFileMapping fileMapping;
    private int file_id = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        
        String fastaFilename = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent().getName());
        
        this.file_id = this.fileMapping.getIDFromFastaFile(fastaFilename);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        int[] arr = new int[2];
        arr[0] = this.file_id;
        arr[1] = value.getPositiveEntriesCount() + value.getNegativeEntriesCount();
        
        context.write(key, new CompressedIntArrayWritable(arr));
    }
        
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.fileMapping = null;
    }
}
