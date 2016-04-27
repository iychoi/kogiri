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
package kogiri.mapreduce.libra;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.libra.common.LibraConfig;
import kogiri.mapreduce.libra.kmersimilarity_m.KmerSimilarityMap;
import kogiri.mapreduce.libra.kmersimilarity_r.KmerSimilarityReduce;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Libra {
    private static final Log LOG = LogFactory.getLog(Libra.class);
    
    private static int RUN_MODE_MAP = 0x00;
    private static int RUN_MODE_REDUCE = 0x01;
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static int checkRunMode(String[] args) {
        int runMode = 0;
        for(String arg : args) {
            if(arg.equalsIgnoreCase("map")) {
                runMode = RUN_MODE_MAP;
            } else if(arg.equalsIgnoreCase("reduce")) {
                runMode = RUN_MODE_REDUCE;
            }
        }
        
        return runMode;
    }
    
    private static String[] removeRunMode(String[] args) {
        List<String> param = new ArrayList<String>();
        for(String arg : args) {
            if(!arg.equalsIgnoreCase("map") && !arg.equalsIgnoreCase("reduce")) {
                param.add(arg);
            }
        }
        
        return param.toArray(new String[0]);
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
        
        int runMode = checkRunMode(args);
        String[] params = removeRunMode(args);
        
        LibraConfig lConfig;
        String lConfigPath = getJSONConfigPath(params);
        if(lConfigPath != null) {
            lConfig = LibraConfig.createInstance(new File(lConfigPath));
        } else {
            CommandArgumentsParser<LibraCmdArgs> parser = new CommandArgumentsParser<LibraCmdArgs>();
            LibraCmdArgs cmdParams = new LibraCmdArgs();
            if(!parser.parse(params, cmdParams)) {
                printHelp();
                return;
            }
            
            lConfig = cmdParams.getLibraConfig();
        }
        
        int res = 0;
        if(runMode == RUN_MODE_MAP) {
            KmerSimilarityMap similarity = new KmerSimilarityMap();
            res = similarity.run(lConfig);
        } else if(runMode == RUN_MODE_REDUCE) {
            KmerSimilarityReduce similarity = new KmerSimilarityReduce();
            res = similarity.run(lConfig);
        }

        System.exit(res);
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Kogiri : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("LIBRA - compute similarity");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> kogiri libra [map|reduce] <arguments ...>");
        System.out.println();
        System.out.println("Mode :");
        System.out.println("> map");
        System.out.println("> \tcompute similarity using mappers");
        System.out.println("> reduce");
        System.out.println("> \tcompute similarity using reducers");
    }
}
