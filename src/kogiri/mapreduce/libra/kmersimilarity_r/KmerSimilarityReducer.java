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
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.common.kmermatch.KmerMatchFileMapping;
import kogiri.mapreduce.libra.common.LibraConfig;
import kogiri.mapreduce.libra.common.kmersimilarity.KmerSimilarityOutputRecord;
import kogiri.mapreduce.preprocess.common.helpers.KmerStatisticsHelper;
import kogiri.mapreduce.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityReducer extends Reducer<CompressedSequenceWritable, CompressedIntArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarityReducer.class);
    
    private LibraConfig libraConfig;
    private JsonSerializer serializer;
    private KmerMatchFileMapping fileMapping;
    private int valuesLen;
    private double[] scoreAccumulated;
    private double[] tfConsineNormBase;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.libraConfig = LibraConfig.createInstance(context.getConfiguration());
        this.serializer = new JsonSerializer();
        
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
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
    protected void reduce(CompressedSequenceWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        // compute normal
        double[] normal = new double[this.valuesLen];
        for(int i=0;i<this.valuesLen;i++) {
            normal[i] = 0;
        }
        
        for(CompressedIntArrayWritable value : values) {
            int[] arr = value.get();
            int file_id = arr[0];
            int freq = arr[1];
            double tf = 1 + Math.log10(freq);
            normal[file_id] = ((double)tf) / this.tfConsineNormBase[file_id];
        }
        
        accumulateScore(normal);
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

        this.libraConfig = null;
        this.serializer = null;
        
        this.fileMapping = null;
    }
}
