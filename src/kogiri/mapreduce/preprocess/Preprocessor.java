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

import java.io.File;
import kogiri.mapreduce.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.indexing.stage1.ReadIndexBuilder;
import kogiri.mapreduce.preprocess.indexing.stage2.KmerIndexBuilder;
import kogiri.mapreduce.preprocess.indexing.stage3.KmerStatisticsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Preprocessor {
    private static final Log LOG = LogFactory.getLog(Preprocessor.class);
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static int getPreprocessorConfigPathIndex(String[] args) {
        for(int i=0;i<args.length;i++) {
            if(args[i].equalsIgnoreCase("--json")) {
                if(args.length >= i+1) {
                    return i+1;
                }
            }
        }
        return -1;
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        }
        
        PreprocessorConfig ppConfig;
        int ppConfigIndex = getPreprocessorConfigPathIndex(args);
        if(ppConfigIndex >= 0) {
            ppConfig = PreprocessorConfig.createInstance(new File(args[ppConfigIndex]));
        } else {
            CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
            PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
            if(!parser.parse(args, cmdParams)) {
                printHelp();
                return;
            }
            
            ppConfig = cmdParams.getPreprocessorConfig();
        }
        
        ReadIndexBuilder stage1 = new ReadIndexBuilder();
        int res = stage1.run(ppConfig);
        if(res == 0) {
            KmerIndexBuilder stage2 = new KmerIndexBuilder();
            res = stage2.run(ppConfig);
            if(res == 0) {
                KmerStatisticsBuilder stage3 = new KmerStatisticsBuilder();
                res = stage3.run(ppConfig);
            }
        }
        
        System.exit(res);
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Kogiri : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Sample Preprocessor");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> kogiri preprocess [stage1|stage2|stage3] <arguments ...>");
        System.out.println();
        System.out.println("Stage :");
        System.out.println("> stage1");
        System.out.println("> \tBuild ReadIndex + Generate k-mer histogram");
        System.out.println("> stage2");
        System.out.println("> \tBuild KmerIndex");
        System.out.println("> stage3");
        System.out.println("> \tBuild KmerStatistics");
    }
}
