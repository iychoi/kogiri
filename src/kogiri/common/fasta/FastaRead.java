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
package kogiri.common.fasta;

/**
 *
 * @author iychoi
 */
public class FastaRead {
    private String filename;
    private long read_offset;
    private String description;
    private String sequence;
    private boolean continuous_read = false;
    
    public FastaRead(String filename) {
        this.filename = filename;
    }
    
    public FastaRead(FastaRawRead rawRead) {
        this.filename = rawRead.getFileName();
        this.read_offset = rawRead.getReadOffset();
        this.description = rawRead.getDescription();
        String pureSequence = new String();
        for (int i = 0; i < rawRead.getRawSequence().length; i++) {
            pureSequence += rawRead.getRawSequence()[i].getLine();
        }
        this.sequence = pureSequence;
        this.continuous_read = rawRead.getContinuousRead();
    }

    public String getFileName() {
        return this.filename;
    }
    
    public void setReadOffset(long offset) {
        this.read_offset = offset;
    }
    
    public long getReadOffset() {
        return this.read_offset;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setSequence(String sequence) {
        this.sequence = sequence;
    }
    
    public String getSequence() {
        return this.sequence;
    }
    
    public void setContinuousRead(boolean continuous_read) {
        this.continuous_read = continuous_read;
    }
    
    public boolean getContinuousRead() {
        return this.continuous_read;
    }
}
