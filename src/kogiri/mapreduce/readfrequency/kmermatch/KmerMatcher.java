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
package kogiri.mapreduce.readfrequency.kmermatch;

import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatcherFileMapping;
import kogiri.mapreduce.readfrequency.common.helpers.KmerMatchHelper;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchInputFormat;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchInputFormatConfig;
import java.io.IOException;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.mapreduce.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.common.helpers.MapReduceClusterHelper;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.readfrequency.ReadFrequencyCounterCmdArgs;
import kogiri.mapreduce.readfrequency.common.IReadFrequencyCounterStage;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConfig;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class KmerMatcher extends Configured implements Tool, IReadFrequencyCounterStage {
    
    private static final Log LOG = LogFactory.getLog(KmerMatcher.class);
    
    private static final int PARTITIONS_PER_CORE = 10;
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerMatcher(), args);
        System.exit(res);
    }
    
    public KmerMatcher() {
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        CommandArgumentsParser<ReadFrequencyCounterCmdArgs> parser = new CommandArgumentsParser<ReadFrequencyCounterCmdArgs>();
        ReadFrequencyCounterCmdArgs cmdParams = new ReadFrequencyCounterCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        ReadFrequencyCounterConfig rfConfig = cmdParams.getReadFrequencyCounterConfig();
        
        return runJob(rfConfig);
    }
    
    @Override
    public int run(ReadFrequencyCounterConfig rfConfig) throws Exception {
        setConf(new Configuration());
        return runJob(rfConfig);
    }
    
    
    private void validateReadFrequencyCounterConfig(ReadFrequencyCounterConfig rfConfig) throws ReadFrequencyCounterConfigException {
        if(rfConfig.getKmerIndexPath().size() <= 0) {
            throw new ReadFrequencyCounterConfigException("cannot find input kmer index path");
        }
        
        if(rfConfig.getClusterConfiguration() == null) {
            throw new ReadFrequencyCounterConfigException("cannout find cluster configuration");
        }
        
        if(rfConfig.getKmerHistogramPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer histogram path");
        }
        
        if(rfConfig.getKmerStatisticsPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer statistics path");
        }
        
        if(rfConfig.getKmerMatchPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer match path");
        }
    }
    
    private int runJob(ReadFrequencyCounterConfig rfConfig) throws Exception {
        // check config
        validateReadFrequencyCounterConfig(rfConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        Job job = new Job(conf, "Kogiri Read Frequency Counter - Finding Matching Kmers");
        conf = job.getConfiguration();
        
        // set user configuration
        rfConfig.getClusterConfiguration().configureTo(conf);
        rfConfig.saveTo(conf);
        
        job.setJarByClass(KmerMatcher.class);
        
        // Mapper
        job.setMapperClass(KmerMatcherMapper.class);
        job.setInputFormatClass(KmerMatchInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Inputs
        Path[] kmerIndexFiles = KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, rfConfig.getKmerIndexPath());
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
        
        KmerMatcherFileMapping fileMapping = new KmerMatcherFileMapping();
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
        matchInputFormatConfig.setKmerHistogramPath(rfConfig.getKmerHistogramPath());
        matchInputFormatConfig.setKmerStatisticsPath(rfConfig.getKmerStatisticsPath());
        matchInputFormatConfig.setStandardDeviationFactor(rfConfig.getStandardDeviationFactor());
        
        KmerMatchInputFormat.setInputFormatConfig(job, matchInputFormatConfig);
        
        FileOutputFormat.setOutputPath(job, new Path(rfConfig.getKmerMatchPath()));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Map only job
        job.setNumReduceTasks(0);

        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(rfConfig.getKmerMatchPath()), conf);
            
            Path tableFilePath = new Path(rfConfig.getKmerMatchPath(), KmerMatchHelper.makeKmerMatchTableFileName());
            FileSystem fs = tableFilePath.getFileSystem(conf);
            fileMapping.saveTo(fs, tableFilePath);
        }
        
        // report
        if(rfConfig.getReportPath() != null && !rfConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(rfConfig.getReportPath());
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
                    String newName = KmerMatchHelper.makeKmerMatchResultFileName(mapreduceID);
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
