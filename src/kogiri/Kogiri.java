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
package kogiri;

import java.util.ArrayList;
import java.util.List;
import kogiri.common.helpers.ClassHelper;
import kogiri.mapreduce.preprocess.Preprocessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Kogiri {

    private static final Log LOG = LogFactory.getLog(Kogiri.class);
    
    private static final String[] CLASS_SEARCH_PACKAGES = {
        "kogiri.preprocess"
    };
    
    private static void invokeClass(Class clazz, String[] args) throws Exception {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        // invoke main
        ClassHelper.invokeMain(clazz, args);
    }
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static String getTargetClassName(String[] args) {
        if(args.length < 1) {
            return null;
        }
        
        return args[0];
    }
    
    private static String[] getTargetClassArguments(String[] args) {
        List<String> targetClassArguments = new ArrayList<String>();
        if(args.length > 1) {
            for(int i=1; i<args.length; i++) {
                targetClassArguments.add(args[i]);
            }
        }
        
        return targetClassArguments.toArray(new String[0]);
    }
    
    private static Class getClassFromCommand(String command) {
        if(command.equalsIgnoreCase("preprocess")) {
            return Preprocessor.class;
        } else {
            return null;
        }
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        } 
        
        String potentialClassName = getTargetClassName(args);
        if(potentialClassName != null) {
            Class clazz = null;
            try {
                clazz = ClassHelper.findClass(potentialClassName, CLASS_SEARCH_PACKAGES);
            } catch (ClassNotFoundException ex) {
            }

            if(clazz == null) {
                clazz = getClassFromCommand(potentialClassName);
            }

            if(clazz != null) {
                String[] classArg = getTargetClassArguments(args);
                // call a main function in the class
                invokeClass(clazz, classArg);
            } else {
                System.err.println("Class name or command is not given properly : " + potentialClassName);
            }
        } else {
            printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Kogiri : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> kogiri <classpath|command> <arguments ...>");
        System.out.println();
        System.out.println("Commands :");
        System.out.println("> preprocess");
    }
}
