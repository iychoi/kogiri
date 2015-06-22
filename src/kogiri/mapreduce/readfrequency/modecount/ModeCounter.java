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
package kogiri.mapreduce.readfrequency.modecount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.MultiFileIntWritable;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.mapreduce.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
import kogiri.mapreduce.common.namedoutput.NamedOutputRecord;
import kogiri.mapreduce.common.namedoutput.NamedOutputs;
import kogiri.mapreduce.readfrequency.ReadFrequencyCounterCmdArgs;
import kogiri.mapreduce.readfrequency.common.IReadFrequencyCounterStage;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConfig;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConfigException;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConstants;
import kogiri.mapreduce.readfrequency.common.helpers.KmerMatchHelper;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatcherFileMapping;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class ModeCounter extends Configured implements Tool, IReadFrequencyCounterStage {
    
    private static final Log LOG = LogFactory.getLog(ModeCounter.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ModeCounter(), args);
        System.exit(res);
    }
    
    public ModeCounter() {
        
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
        if(rfConfig.getClusterConfiguration() == null) {
            throw new ReadFrequencyCounterConfigException("cannout find cluster configuration");
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
        
        // set user configuration
        rfConfig.getClusterConfiguration().configureTo(conf);
        rfConfig.saveTo(conf);
        
        // table file
        Path tableFilePath = new Path(rfConfig.getKmerMatchPath(), KmerMatchHelper.makeKmerMatchTableFileName());
        FileSystem fs = tableFilePath.getFileSystem(conf);
        KmerMatcherFileMapping fileMapping = KmerMatcherFileMapping.createInstance(fs, tableFilePath);
        
        Path[] inputFiles = KmerMatchHelper.getAllKmerMatchResultFilePath(conf, rfConfig.getKmerMatchPath());
        
        // Register named outputs
        NamedOutputs namedOutputs = new NamedOutputs();
        for(int i=0;i<fileMapping.getSize();i++) {
            String fastaFileName = fileMapping.getFastaFileFromID(i);
            namedOutputs.add(fastaFileName);
        }
        namedOutputs.saveTo(conf);
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<fileMapping.getSize();round++) {
            String roundOutputPath = rfConfig.getReadFrequencyPath() + "_round" + round;
            
            Job job = new Job(conf, "Kogiri Preprocessor - Computing Mode of Kmer Frequency (" + round + " of " + fileMapping.getSize() + ")");
            job.setJarByClass(ModeCounter.class);
            
            // Mapper
            job.setMapperClass(ModeCounterMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(MultiFileIntWritable.class);
            job.setMapOutputValueClass(CompressedIntArrayWritable.class);
            
            // Combiner
            job.setCombinerClass(ModeCounterCombiner.class);
            
            // Partitioner
            job.setPartitionerClass(ModeCounterPartitioner.class);
            
            // Reducer
            job.setReducerClass(ModeCounterReducer.class);
            
            // Specify key / value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            // Inputs
            FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
            
            ModeCounterConfig modeCounterConfig = new ModeCounterConfig();
            modeCounterConfig.setMasterFileID(round);
            modeCounterConfig.saveTo(job.getConfiguration());
            
            FileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
            job.setOutputFormatClass(TextOutputFormat.class);
            
            for (NamedOutputRecord namedOutput : namedOutputs.getRecord()) {
                MultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), TextOutputFormat.class, Text.class, Text.class);
            }
            
            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            jobs.add(job);

            // commit results
            if (result) {
                commitRoundOutputFiles(new Path(roundOutputPath), new Path(rfConfig.getReadFrequencyPath()), job.getConfiguration(), namedOutputs, round);
            }
            
            if(!result) {
                LOG.error("job failed at round " + round + " of " + fileMapping.getSize());
                job_result = false;
                break;
            }
        }
        
        // report
        if(rfConfig.getReportPath() != null && !rfConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(jobs);
            report.writeTo(rfConfig.getReportPath());
        }
        
        return job_result ? 0 : 1;
    }
    
    private void commitRoundOutputFiles(Path MROutputPath, Path finalOutputPath, Configuration conf, NamedOutputs namedOutputs, int round) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        NamedOutputRecord roundMasterRecord = namedOutputs.getRecordFromID(round);
        Path roundDestPath = new Path(finalOutputPath, roundMasterRecord.getFilename());
        if(!fs.exists(roundDestPath)) {
            fs.mkdirs(roundDestPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath);
                    if(namedOutput != null) {
                        Path toPath = new Path(roundDestPath, namedOutput.getFilename() + "." + ReadFrequencyCounterConstants.READ_FREQUENCY_FILENAME_FILENAME_EXTENSION);
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + MROutputPath.toString());
        }
        
        fs.delete(MROutputPath, true);
    }
}
