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
package kogiri.mapreduce.libra.kmersimilarity;

import java.io.IOException;
import kogiri.common.hadoop.io.datatypes.DoubleArrayWritable;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.common.helpers.MapReduceClusterHelper;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
import kogiri.mapreduce.common.kmermatch.KmerMatchFileMapping;
import kogiri.mapreduce.common.kmermatch.KmerMatchInputFormat;
import kogiri.mapreduce.common.kmermatch.KmerMatchInputFormatConfig;
import kogiri.mapreduce.libra.LibraCmdArgs;
import kogiri.mapreduce.libra.common.ILibraStage;
import kogiri.mapreduce.libra.common.LibraConfig;
import kogiri.mapreduce.libra.common.LibraConfigException;
import kogiri.mapreduce.libra.common.helpers.KmerSimilarityHelper;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerSimilarity extends Configured implements Tool, ILibraStage {
    
    private static final Log LOG = LogFactory.getLog(KmerSimilarity.class);
    
    private static final int PARTITIONS_PER_CORE = 10;
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerSimilarity(), args);
        System.exit(res);
    }
    
    public KmerSimilarity() {
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        CommandArgumentsParser<LibraCmdArgs> parser = new CommandArgumentsParser<LibraCmdArgs>();
        LibraCmdArgs cmdParams = new LibraCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        LibraConfig lConfig = cmdParams.getLibraConfig();
        
        return runJob(lConfig);
    }
    
    @Override
    public int run(LibraConfig lConfig) throws Exception {
        setConf(new Configuration());
        return runJob(lConfig);
    }
    
    
    private void validateLibraConfig(LibraConfig lConfig) throws LibraConfigException {
        if(lConfig.getKmerIndexPath().size() <= 0) {
            throw new LibraConfigException("cannot find input kmer index path");
        }
        
        if(lConfig.getClusterConfiguration() == null) {
            throw new LibraConfigException("cannout find cluster configuration");
        }
        
        if(lConfig.getKmerHistogramPath() == null) {
            throw new LibraConfigException("cannot find kmer histogram path");
        }
        
        if(lConfig.getKmerStatisticsPath() == null) {
            throw new LibraConfigException("cannot find kmer statistics path");
        }
        
        if(lConfig.getOutputPath() == null) {
            throw new LibraConfigException("cannot find output path");
        }
    }
    
    private int runJob(LibraConfig lConfig) throws Exception {
        // check config
        validateLibraConfig(lConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        Job job = new Job(conf, "Kogiri Libra - Computing similarity between samples");
        conf = job.getConfiguration();
        
        // set user configuration
        lConfig.getClusterConfiguration().configureTo(conf);
        lConfig.saveTo(conf);
        
        job.setJarByClass(KmerSimilarity.class);
        
        // Mapper
        job.setMapperClass(KmerSimilarityMapper.class);
        job.setInputFormatClass(KmerMatchInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // Combiner
        //job.setCombinerClass(KmerSimilarityCombiner.class);
        
        // Reducer
        //job.setReducerClass(KmerSimilarityReducer.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Inputs
        Path[] kmerIndexFiles = KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, lConfig.getKmerIndexPath());
        KmerMatchInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(kmerIndexFiles));

        LOG.info("Input kmer index files : " + kmerIndexFiles.length);
        for(Path inputFile : kmerIndexFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        int kmerSize = 0;
        for(Path inputFile : kmerIndexFiles) {
            // check kmerSize
            int myKmerSize = KmerIndexHelper.getKmerSize(inputFile);
            if(kmerSize == 0) {
                kmerSize = myKmerSize;
            } else {
                if(kmerSize != myKmerSize) {
                    throw new Exception("kmer size must be the same over all given kmer indices");
                }
            }
        }
        
        KmerMatchFileMapping fileMapping = new KmerMatchFileMapping();
        for(Path kmerIndexFile : kmerIndexFiles) {
            String fastaFilename = KmerIndexHelper.getFastaFileName(kmerIndexFile.getName());
            fileMapping.addFastaFile(fastaFilename);
        }
        fileMapping.saveTo(conf);
        
        int MRNodes = MapReduceClusterHelper.getNodeNum(conf);
        
        LOG.info("MapReduce nodes detected : " + MRNodes);

        KmerMatchInputFormatConfig matchInputFormatConfig = new KmerMatchInputFormatConfig();
        matchInputFormatConfig.setKmerSize(kmerSize);
        matchInputFormatConfig.setPartitionNum(MRNodes * PARTITIONS_PER_CORE);
        matchInputFormatConfig.setKmerHistogramPath(lConfig.getKmerHistogramPath());
        matchInputFormatConfig.setKmerStatisticsPath(lConfig.getKmerStatisticsPath());
        matchInputFormatConfig.setStandardDeviationFactor(lConfig.getStandardDeviationFactor());
        
        KmerMatchInputFormat.setInputFormatConfig(job, matchInputFormatConfig);
        
        FileOutputFormat.setOutputPath(job, new Path(lConfig.getOutputPath()));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(lConfig.getOutputPath()), conf);
            
            Path tableFilePath = new Path(lConfig.getOutputPath(), KmerSimilarityHelper.makeKmerSimilarityTableFileName());
            FileSystem fs = tableFilePath.getFileSystem(conf);
            fileMapping.saveTo(fs, tableFilePath);
        }
        
        // report
        if(lConfig.getReportPath() != null && !lConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(lConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
    
    private void commit(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        FileStatus status = fs.getFileStatus(outputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    String newName = KmerSimilarityHelper.makeKmerSimilarityResultFileName(mapreduceID);
                    Path toPath = new Path(entryPath.getParent(), newName);

                    LOG.info("output : " + entryPath.toString());
                    LOG.info("renamed to : " + toPath.toString());
                    fs.rename(entryPath, toPath);
                } else {
                    // let it be
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
}
