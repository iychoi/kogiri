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
package kogiri.common.hadoop.io.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import static org.apache.hadoop.io.WritableComparator.compareBytes;

/**
 *
 * @author iychoi
 */
public class MultiFileIntWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(MultiFileIntWritable.class);
    
    private int fileID;
    private int value;
    private byte[] fullLine;
    
    private static final int ID_BYTES = 2+4;
    
    public MultiFileIntWritable() {}
    
    public MultiFileIntWritable(int fileID, int value) throws IOException { set(fileID, value); }
    
    /**
     * Set the value.
     */
    public void set(int fileID, int value) {
        this.fileID = fileID;
        this.value = value;
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.value >> 24) & 0xff);
        this.fullLine[3] = (byte) ((this.value >> 16) & 0xff);
        this.fullLine[4] = (byte) ((this.value >> 8) & 0xff);
        this.fullLine[5] = (byte) (this.value & 0xff);
    }
    
    /**
     * Return the value.
     */
    public int getFileID() {
        return this.fileID;
    }
    
    public int getValue() {
        return this.value;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.fileID = in.readShort();
        this.value = in.readInt();
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.value >> 24) & 0xff);
        this.fullLine[3] = (byte) ((this.value >> 16) & 0xff);
        this.fullLine[4] = (byte) ((this.value >> 8) & 0xff);
        this.fullLine[5] = (byte) (this.value & 0xff);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.fileID);
        out.writeInt(this.value);
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiFileIntWritable) {
            return super.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public String toString() {
        return this.fileID + ":" + this.value;
    }

    @Override
    public int getLength() {
        return this.fullLine.length;
    }

    @Override
    public byte[] getBytes() {
        return this.fullLine;
    }
    
    /** A Comparator optimized for MultiFileIntWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(MultiFileIntWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1,
                    b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(MultiFileIntWritable.class, new Comparator());
    }
}
