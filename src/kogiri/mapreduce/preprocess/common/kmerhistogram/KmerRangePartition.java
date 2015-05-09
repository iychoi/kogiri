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
package kogiri.mapreduce.preprocess.common.kmerhistogram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import kogiri.common.helpers.SequenceHelper;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerRangePartition {
    private int kmerSize;
    private int numPartitions;
    private int partitionIndex;
    private BigInteger partitionSize;
    private BigInteger partitionBegin;
    private BigInteger parititionEnd;
    
    public KmerRangePartition() {
    }
    
    public KmerRangePartition(int kmerSize, int numPartition, int partitionIndex, BigInteger partitionSize, BigInteger partitionBegin, BigInteger partitionEnd) {
        this.kmerSize = kmerSize;
        this.numPartitions = numPartition;
        this.partitionIndex = partitionIndex;
        this.partitionSize = partitionSize;
        this.partitionBegin = partitionBegin;
        this.parititionEnd = partitionEnd;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public int getNumPartitions() {
        return this.numPartitions;
    }
    
    public int getPartitionIndex() {
        return this.partitionIndex;
    }
    
    public BigInteger getPartitionSize() {
        return this.partitionSize;
    }
    
    public BigInteger getPartitionBegin() {
        return this.partitionBegin;
    }
    
    public String getPartitionBeginKmer() {
        return SequenceHelper.convertToString(this.partitionBegin, this.kmerSize);
    }
    
    public BigInteger getPartitionEnd() {
        return this.parititionEnd;
    }
    
    public String getPartitionEndKmer() {
        return SequenceHelper.convertToString(this.parititionEnd, this.kmerSize);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerSize);
        out.writeInt(this.numPartitions);
        out.writeInt(this.partitionIndex);
        Text.writeString(out, this.partitionSize.toString());
        Text.writeString(out, this.partitionBegin.toString());
        Text.writeString(out, this.parititionEnd.toString());
    }

    public void read(DataInput in) throws IOException {
        this.kmerSize = in.readInt();
        this.numPartitions = in.readInt();
        this.partitionIndex = in.readInt();
        this.partitionSize = new BigInteger(Text.readString(in));
        this.partitionBegin = new BigInteger(Text.readString(in));
        this.parititionEnd = new BigInteger(Text.readString(in));
    }
    
    @Override
    public String toString() {
        return "kmerSize : " + this.kmerSize + ", numPartition : " + this.numPartitions + ", partitionIndex : " + this.partitionIndex +
                ", partitionSize : " + this.partitionSize.toString() + ", beginKmer : " + SequenceHelper.convertToString(this.partitionBegin, this.kmerSize) + ", endKmer : " + SequenceHelper.convertToString(this.parititionEnd, this.kmerSize);
    }
}
