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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class FilteredKmerIndexReader extends AKmerIndexReader {

    private static final Log LOG = LogFactory.getLog(FilteredKmerIndexReader.class);
    
    private AKmerIndexReader kmerIndexReader;
    private AKmerIndexRecordFilter recordFilter;
    
    public FilteredKmerIndexReader(FileSystem fs, Path kmerIndexPath, AKmerIndexRecordFilter filter, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, null, null, filter, conf);
    }
    
    public FilteredKmerIndexReader(FileSystem fs, Path kmerIndexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, AKmerIndexRecordFilter filter, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, beginKey, endKey, filter, conf);
    }
    
    public FilteredKmerIndexReader(FileSystem fs, Path kmerIndexPath, String beginKey, String endKey, AKmerIndexRecordFilter filter, Configuration conf) throws IOException {
        initialize(fs, kmerIndexPath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), filter, conf);
    }
    
    private void initialize(FileSystem fs, Path kmerIndexPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, AKmerIndexRecordFilter filter, Configuration conf) throws IOException {
        this.kmerIndexReader = new KmerIndexReader(fs, kmerIndexPath, beginKey, endKey, conf);
        this.recordFilter = filter;
    }
    
    @Override
    public Path getIndexPath() {
        return this.kmerIndexReader.getIndexPath();
    }

    @Override
    public boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException {
        CompressedSequenceWritable tempKey = new CompressedSequenceWritable();
        CompressedIntArrayWritable tempVal = new CompressedIntArrayWritable();
        
        if(this.kmerIndexReader.next(tempKey, tempVal)) {
            if(this.recordFilter != null) {
                CompressedIntArrayWritable newVal = this.recordFilter.accept(tempKey, tempVal);
                key.set(tempKey);
                val.set(newVal);
                return true;
            }
        }
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.kmerIndexReader.close();
    }
}
