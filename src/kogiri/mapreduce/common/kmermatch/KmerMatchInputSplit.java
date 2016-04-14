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
package kogiri.mapreduce.common.kmermatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerRangePartition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputSplit extends InputSplit implements Writable {

    private Path[] kmerIndexPath;
    private KmerRangePartition partition;

    public KmerMatchInputSplit() {    
    }
    
    public KmerMatchInputSplit(Path[] kmerIndexFilePath, KmerRangePartition partition) {
        this.kmerIndexPath = kmerIndexFilePath;
        this.partition = partition;
    }
    
    public Path[] getIndexFilePath() {
        return this.kmerIndexPath;
    }
    
    public KmerRangePartition getPartition() {
        return this.partition;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Path path : this.kmerIndexPath) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(path.toString());
        }
        return this.partition.toString() + "\n" + sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {"localhost"};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerIndexPath.length);
        for (Path indexPath : this.kmerIndexPath) {
            Text.writeString(out, indexPath.toString());
        }
        this.partition.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerIndexPath = new Path[in.readInt()];
        for(int i=0;i<this.kmerIndexPath.length;i++) {
            this.kmerIndexPath[i] = new Path(Text.readString(in));
        }
        this.partition = new KmerRangePartition();
        this.partition.read(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.partition.getPartitionSize().longValue();
    }
}
