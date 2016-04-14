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
import java.util.List;
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
public class DoubleArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(DoubleArrayWritable.class);
    
    private double[] doubleArray;
    
    private byte[] prevBytes;
    
    public DoubleArrayWritable() {}
    
    public DoubleArrayWritable(double[] doubleArray) { set(doubleArray); }
    
    public DoubleArrayWritable(List<Double> doubleArray) { set(doubleArray); }
    
    /**
     * Set the value.
     */
    public void set(double[] doubleArray) {
        this.doubleArray = doubleArray;
        this.prevBytes = null;
    }
    
    public void set(List<Double> doubleArray) {
        double[] arr = new double[doubleArray.size()];
        for(int i=0;i<doubleArray.size();i++) {
            arr[i] = doubleArray.get(i);
        }
        this.doubleArray = arr;
        this.prevBytes = null;
    }
    
    public void set(DoubleArrayWritable that) {
        this.doubleArray = that.doubleArray;
        this.prevBytes = that.prevBytes;
    }
    
    public void setEmpty() {
        this.doubleArray = null;
        this.prevBytes = null;
    }
    
    public boolean isEmpty() {
        if(this.doubleArray == null) {
            return true;
        }
        return false;
    }

    /**
     * Return the value.
     */
    public double[] get() {
        return this.doubleArray;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        
        double[] arr = new double[count];
        for (int i = 0; i < count; i++) {
            arr[i] = in.readDouble();
        }
        
        this.doubleArray = arr;
        this.prevBytes = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = this.doubleArray.length;
        out.writeInt(count);
        
        for (int i = 0; i < count; i++) {
            out.writeDouble((double)this.doubleArray[i]);
        }
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof DoubleArrayWritable) {
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
        String value = new String();
        
        for(int i=0;i<this.doubleArray.length;i++) {
            if(value.length() != 0) {
                value += ",";
            }
            value += this.doubleArray[i];
        }
        return value;
    }
    
    @Override
    public int getLength() {
        return this.doubleArray.length * 8;
    }

    @Override
    public byte[] getBytes() {
        if(this.prevBytes == null) {
            byte[] arr = new byte[this.doubleArray.length * 8];
            for(int i=0;i<this.doubleArray.length;i++) {
                double dvalue = this.doubleArray[i];
                long lvalue = Double.doubleToLongBits(dvalue);
                arr[4*i] = (byte) ((lvalue >> 56) & 0xff);
                arr[4*i+1] = (byte) ((lvalue >> 48) & 0xff);
                arr[4*i+2] = (byte) ((lvalue >> 40) & 0xff);
                arr[4*i+3] = (byte) ((lvalue >> 32) & 0xff);
                arr[4*i+4] = (byte) ((lvalue >> 24) & 0xff);
                arr[4*i+5] = (byte) ((lvalue >> 16) & 0xff);
                arr[4*i+6] = (byte) ((lvalue >> 8) & 0xff);
                arr[4*i+7] = (byte) (lvalue & 0xff);
            }
            this.prevBytes = arr;
        }
        return prevBytes;
    }
    
    /** A Comparator optimized for IntArrayWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(DoubleArrayWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1,
                    b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(DoubleArrayWritable.class, new DoubleArrayWritable.Comparator());
    }
}
