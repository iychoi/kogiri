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
package kogiri.mapreduce.preprocess.indexing.stage1;

import kogiri.mapreduce.preprocess.common.helpers.KmerHistogramHelper;
import java.io.IOException;
import kogiri.common.fasta.FastaRead;
import kogiri.mapreduce.common.namedoutput.NamedOutputRecord;
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.kmerhistogram.KmerHistogram;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class ReadIndexBuilderMapper extends Mapper<LongWritable, FastaRead, LongWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(ReadIndexBuilderMapper.class);
    
    private PreprocessorConfig ppConfig;
    
    private NamedOutputs namedOutputs;
    private MultipleOutputs mos;
    private int readIDCounter;
    private KmerHistogram histogram;
    private String namedOutput;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.ppConfig = PreprocessorConfig.createInstance(conf);
        this.mos = new MultipleOutputs(context);
        this.namedOutputs = NamedOutputs.createInstance(conf);
        
        this.readIDCounter = 0;
        
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        
        int namedoutputID = this.namedOutputs.getIDFromFilename(inputSplit.getPath().getName());
        NamedOutputRecord namedoutputRecord = this.namedOutputs.getRecordFromID(namedoutputID);
        this.namedOutput = namedoutputRecord.getIdentifier();
        
        this.histogram = new KmerHistogram(namedoutputRecord.getFilename(), this.ppConfig.getKmerSize());
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        this.readIDCounter++;
        
        this.mos.write(this.namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDCounter));
        
        this.histogram.takeSample(value.getSequence());
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(this.mos != null) {
            this.mos.close();
        }
        
        String sampleName = this.histogram.getSampleName();
        String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(sampleName);

        LOG.info("create a k-mer histogram file : " + histogramFileName);
        Path histogramOutputFile = new Path(this.ppConfig.getKmerHistogramPath(), histogramFileName);
        FileSystem outputFileSystem = histogramOutputFile.getFileSystem(context.getConfiguration());

        this.histogram.saveTo(outputFileSystem, histogramOutputFile);
    }
}
