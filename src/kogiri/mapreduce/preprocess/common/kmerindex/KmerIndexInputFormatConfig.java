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

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerIndexInputFormatConfig {
    private final static String CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE = "edu.arizona.cs.mrpkm.kmeridx.kmer_size";
    private final static String CONF_KMER_INDEX_CHUNK_INFO_PATH = "edu.arizona.cs.mrpkm.kmeridx.kmeridx_chunkinfo";
    
    private int kmerSize;
    private String kmerIndexChunkInfoPath;
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public void setKmerIndexChunkInfoPath(String kmerIndexChunkInfoPath) {
        this.kmerIndexChunkInfoPath = kmerIndexChunkInfoPath;
    }
    
    public String getKmerIndexChunkInfoPath() {
        return this.kmerIndexChunkInfoPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, this.kmerSize);
        conf.set(CONF_KMER_INDEX_CHUNK_INFO_PATH, this.kmerIndexChunkInfoPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, 0);
        this.kmerIndexChunkInfoPath = conf.get(CONF_KMER_INDEX_CHUNK_INFO_PATH);
    }
}
