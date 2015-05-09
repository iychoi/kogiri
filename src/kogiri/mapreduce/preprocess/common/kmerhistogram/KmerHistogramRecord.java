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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerHistogramRecord implements Comparable<KmerHistogramRecord> {
    private String kmer;
    private long frequency;
    
    public KmerHistogramRecord() {
        
    }
    
    public KmerHistogramRecord(String key, long freqeuncy) {
        this.kmer = key;
        this.frequency = freqeuncy;
    }
    
    @JsonProperty("kmer")
    public String getKmer() {
        return this.kmer;
    }
    
    @JsonProperty("kmer")
    public void setKmer(String kmer) {
        this.kmer = kmer;
    }
    
    @JsonProperty("frequency")
    public long getFrequency() {
        return this.frequency;
    }
    
    @JsonProperty("frequency")
    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }
    
    @JsonIgnore
    public void increaseFrequency(long frequency) {
        this.frequency += frequency;
    }
    
    @JsonIgnore
    public void increaseFrequency() {
        this.frequency++;
    }

    @JsonIgnore
    @Override
    public int compareTo(KmerHistogramRecord right) {
        return this.kmer.compareToIgnoreCase(right.kmer);
    }
}
