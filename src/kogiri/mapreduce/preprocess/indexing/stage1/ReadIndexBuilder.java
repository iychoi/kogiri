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
import kogiri.mapreduce.preprocess.common.helpers.ReadIndexHelper;
import kogiri.mapreduce.preprocess.common.IPreprocessorStage;
import java.io.IOException;
import kogiri.common.hadoop.io.format.fasta.FastaReadInputFormat;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.mapreduce.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
import kogiri.mapreduce.common.namedoutput.NamedOutputRecord;
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import kogiri.mapreduce.preprocess.PreprocessorCmdArgs;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.PreprocessorConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class ReadIndexBuilder extends Configured implements Tool, IPreprocessorStage {
    
    private static final Log LOG = LogFactory.getLog(ReadIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadIndexBuilder(), args);
        System.exit(res);
    }
    
    public ReadIndexBuilder() {
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
        PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        PreprocessorConfig ppConfig = cmdParams.getPreprocessorConfig();
        
        return runJob(ppConfig);
    }
    
    @Override
    public int run(PreprocessorConfig ppConfig) throws Exception {
        setConf(new Configuration());
        return runJob(ppConfig);
    }
    
    private void validatePreprocessorConfig(PreprocessorConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getFastaPath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getClusterConfiguration() == null) {
            throw new PreprocessorConfigException("cannout find cluster configuration");
        }
        
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
        
        if(ppConfig.getReadIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find read index path");
        }
    }
    
    private int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        Job job = new Job(conf, "Kogiri Preprocessor - Building Read Indices");
        conf = job.getConfiguration();
        
        // set user configuration
        ppConfig.getClusterConfiguration().configureTo(conf);
        ppConfig.saveTo(conf);
        
        job.setJarByClass(ReadIndexBuilder.class);

        // Mapper
        job.setMapperClass(ReadIndexBuilderMapper.class);
        FastaReadInputFormat.setSplitable(conf, false);
        job.setInputFormatClass(FastaReadInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Inputs
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePath(conf, ppConfig.getFastaPath());
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        LOG.info("Input sample files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        // Register named outputs
        NamedOutputs namedOutputs = new NamedOutputs();
        namedOutputs.add(inputFiles);
        namedOutputs.saveTo(conf);
        
        FileOutputFormat.setOutputPath(job, new Path(ppConfig.getReadIndexPath()));
        job.setOutputFormatClass(MapFileOutputFormat.class);
        
        for(NamedOutputRecord namedOutput : namedOutputs.getRecord()) {
            MultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), MapFileOutputFormat.class, LongWritable.class, IntWritable.class);
        }

        // Map only job
        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(ppConfig.getReadIndexPath()), conf, namedOutputs);
            commit(new Path(ppConfig.getKmerHistogramPath()), conf, namedOutputs);
        }
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }

    private void commit(Path outputPath, Configuration conf, NamedOutputs namedOutputs) throws IOException {
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
                    fs.delete(entryPath, true);
                } else if(KmerHistogramHelper.isKmerHistogramFile(entryPath)) {
                    // not necessary
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath.getName());
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), ReadIndexHelper.makeReadIndexFileName(namedOutput.getFilename()));
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
}
