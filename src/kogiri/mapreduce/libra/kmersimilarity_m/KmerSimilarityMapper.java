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
package kogiri.mapreduce.libra.kmersimilarity_m;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.common.kmermatch.KmerMatchFileMapping;
import kogiri.mapreduce.common.kmermatch.KmerMatchResult;
import kogiri.mapreduce.libra.common.LibraConfig;
import kogiri.mapreduce.libra.common.kmersimilarity.KmerSimilarityOutputRecord;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.preprocess.common.helpers.KmerStatisticsHelper;
import kogiri.mapreduce.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMapper.class);
    
    private LibraConfig libraConfig;
    private KmerMatchFileMapping fileMapping;
    private Hashtable<String, Integer> idCacheTable;
    private Counter reportCounter;
    private JsonSerializer serializer;
    
    private int valuesLen;
    private double[] scoreAccumulated;
    private double[] tfConsineNormBase;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.libraConfig = LibraConfig.createInstance(context.getConfiguration());
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        this.idCacheTable = new Hashtable<String, Integer>();
        
        this.reportCounter = context.getCounter("KmerSimilarity", "report");
        this.serializer = new JsonSerializer();
        
        this.valuesLen = this.fileMapping.getSize();
        this.scoreAccumulated = new double[this.valuesLen * this.valuesLen];
        for(int i=0;i<this.scoreAccumulated.length;i++) {
            this.scoreAccumulated[i] = 0;
        }
        
        this.tfConsineNormBase = new double[this.valuesLen];
        for(int i=0;i<this.tfConsineNormBase.length;i++) {
            // fill tfConsineNormBase
            String fastaFilename = this.fileMapping.getFastaFileFromID(i);
            String statisticsFilename = KmerStatisticsHelper.makeKmerStatisticsFileName(fastaFilename);
            Path statisticsPath = new Path(this.libraConfig.getKmerStatisticsPath(), statisticsFilename);
            FileSystem fs = statisticsPath.getFileSystem(context.getConfiguration());
            
            KmerStatistics statistics = KmerStatistics.createInstance(fs, statisticsPath);

            this.tfConsineNormBase[i] = statistics.getTFCosineNormBase();
        }
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        CompressedIntArrayWritable[] valueArray = value.getVals();
        Path[] kmerIndexPathArray = value.getKmerIndexPath();
        
        // filter out empty values
        ArrayList<CompressedIntArrayWritable> filteredValueArray = new ArrayList<CompressedIntArrayWritable>();
        ArrayList<Path> filteredKmerIndexPathArray = new ArrayList<Path>();
        
        for(int i=0;i<valueArray.length;i++) {
            if(!valueArray[i].isEmpty()) {
                filteredValueArray.add(valueArray[i]);
                filteredKmerIndexPathArray.add(kmerIndexPathArray[i]);
            }
        }
        
        valueArray = null;
        kmerIndexPathArray = null;
        
        if(filteredValueArray.size() <= 1) {
            // skip
            return;
        }
        
        int[] fileid_arr = new int[filteredValueArray.size()];
        
        for(int i=0;i<filteredValueArray.size();i++) {
            int fileidInt = 0;
            String indexFilename = filteredKmerIndexPathArray.get(i).getName();
            Integer fileid = this.idCacheTable.get(indexFilename);
            if(fileid == null) {
                String fastaFilename = KmerIndexHelper.getFastaFileName(indexFilename);
                int id = this.fileMapping.getIDFromFastaFile(fastaFilename);
                this.idCacheTable.put(indexFilename, id);
                fileidInt = id;
            } else {
                fileidInt = fileid.intValue();
            }
            
            fileid_arr[i] = fileidInt;
        }
        
        // compute normal
        double[] normal = new double[this.valuesLen];
        for(int i=0;i<this.valuesLen;i++) {
            normal[i] = 0;
        }
        
        for(int i=0;i<filteredValueArray.size();i++) {
            CompressedIntArrayWritable arr = filteredValueArray.get(i);
            int freq = arr.getNegativeEntriesCount() + arr.getPositiveEntriesCount();
            double tf = 1 + Math.log10(freq);
            normal[fileid_arr[i]] = ((double)tf) / this.tfConsineNormBase[fileid_arr[i]];
        }
        
        accumulateScore(normal);
        
        this.reportCounter.increment(1);
    }
    
    private void accumulateScore(double[] normal) {
        for(int i=0;i<this.valuesLen;i++) {
            for(int j=0;j<this.valuesLen;j++) {
                this.scoreAccumulated[i*this.valuesLen + j] += normal[i] * normal[j];
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        KmerSimilarityOutputRecord rec = new KmerSimilarityOutputRecord();
        rec.setScore(this.scoreAccumulated);
                
        String json = this.serializer.toJson(rec);
        context.write(new Text(" "), new Text(json));

        this.fileMapping = null;
        this.idCacheTable.clear();
        this.idCacheTable = null;
    
        this.libraConfig = null;
        this.scoreAccumulated = null;
        this.tfConsineNormBase = null;
        
        this.serializer = null;
    }
}
