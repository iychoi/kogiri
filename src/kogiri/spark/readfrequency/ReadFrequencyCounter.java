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
package kogiri.spark.readfrequency;

import java.io.File;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.spark.readfrequency.common.ReadFrequencyCounterConfig;
import kogiri.spark.readfrequency.kmermatch.KmerMatcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ReadFrequencyCounter {
    private static final Log LOG = LogFactory.getLog(ReadFrequencyCounter.class);
    
    private static int RUN_STAGE_1 = 0x01;
    private static int RUN_STAGE_2 = 0x02;
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static int checkRunStages(String[] args) {
        int runStages = 0;
        for(String arg : args) {
            if(arg.equalsIgnoreCase("stage1")) {
                runStages |= RUN_STAGE_1;
            } else if(arg.equalsIgnoreCase("stage2")) {
                runStages |= RUN_STAGE_2;
            }
        }
        
        if(runStages == 0) {
            runStages |= RUN_STAGE_1;
            runStages |= RUN_STAGE_2;
        }
        return runStages;
    }
    
    private static String getJSONConfigPath(String[] args) {
        for(int i=0;i<args.length;i++) {
            if(args[i].equalsIgnoreCase("--json")) {
                if(args.length >= i+1) {
                    return args[i+1];
                }
            }
        }
        return null;
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        }
        
        ReadFrequencyCounterConfig rfConfig;
        String rfConfigPath = getJSONConfigPath(args);
        if(rfConfigPath != null) {
            rfConfig = ReadFrequencyCounterConfig.createInstance(new File(rfConfigPath));
        } else {
            CommandArgumentsParser<ReadFrequencyCounterCmdArgs> parser = new CommandArgumentsParser<ReadFrequencyCounterCmdArgs>();
            ReadFrequencyCounterCmdArgs cmdParams = new ReadFrequencyCounterCmdArgs();
            if(!parser.parse(args, cmdParams)) {
                printHelp();
                return;
            }
            
            rfConfig = cmdParams.getReadFrequencyCounterConfig();
        }
        
        int runStages = checkRunStages(args);
        
        int res = 0;
        if((runStages & RUN_STAGE_1) == RUN_STAGE_1 &&
                res == 0) {
            res = KmerMatcher.run(rfConfig);
        }
        /*
        if((runStages & RUN_STAGE_2) == RUN_STAGE_2 &&
                res == 0) {
            ModeCounter stage2 = new ModeCounter();
            res = stage2.run(rfConfig);
        }
        */

        System.exit(res);
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Kogiri : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Sample Read Frequency Counter");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> kogiri readfreqcounter [stage1|stage2] <arguments ...>");
        System.out.println();
        System.out.println("Stage :");
        System.out.println("> stage1");
        System.out.println("> \tFind matching kmers");
        System.out.println("> stage2");
        System.out.println("> \tCount read frequency");
    }
}
