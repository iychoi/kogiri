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
package kogiri.mapreduce.preprocess;

import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import java.util.ArrayList;
import java.util.List;
import kogiri.hadoop.common.cmdargs.CommandArgumentsBase;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class PreprocessorCmdArgs extends CommandArgumentsBase {
    
    public PreprocessorCmdArgs() {
        
    }
    
    @Option(name = "-k", aliases = "--kmersize", usage = "specify kmer size")
    protected int kmerSize = PreprocessorConfig.DEFAULT_KMERSIZE;

    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @Option(name = "-o", usage = "specify output path")
    private String outputPath = PreprocessorConfig.DEFAULT_OUTPUT_ROOT_PATH;
        
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @Argument(metaVar = "input-path [input-path ...]", usage = "input-paths")
    private List<String> inputPaths = new ArrayList<String>();

    public String[] getInputPaths() {
        if(this.inputPaths.isEmpty()) {
            return new String[0];
        }

        return this.inputPaths.toArray(new String[0]);
    }

    public String getCommaSeparatedInputPath() {
        String[] inputPaths = getInputPaths();
        StringBuilder CSInputPath = new StringBuilder();
        for(String inputpath : inputPaths) {
            if(CSInputPath.length() != 0) {
                CSInputPath.append(",");
            }
            CSInputPath.append(inputpath);
        }
        return CSInputPath.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String arg : this.inputPaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }

            sb.append(arg);
        }

        return "paths = " + sb.toString();
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.kmerSize <= 0 || 
                this.outputPath == null ||
                this.inputPaths == null || 
                this.inputPaths.isEmpty() || 
                this.inputPaths.size() < 1) {
            return false;
        }
        
        return true;
    }
    
    public PreprocessorConfig getPreprocessorConfig() {
        PreprocessorConfig config = new PreprocessorConfig();
        
        config.setReportPath(this.reportfile);
        config.setClusterConfiguration(this.cluster);
        config.setKmerSize(this.kmerSize);
        config.addFastaPath(this.inputPaths);
        config.setOutputRootPath(this.outputPath);
        return config;
    }
}
