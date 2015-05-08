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
package kogiri.mapreduce.common.helpers;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class MapReduceHelper {
    public static String getOutputNameFromMapReduceOutput(Path mapreduceOutputPath) {
        return getOutputNameFromMapReduceOutput(mapreduceOutputPath.getName());
    }
    
    public static String getOutputNameFromMapReduceOutput(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return mapreduceOutputName.substring(0, midx);
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return mapreduceOutputName.substring(0, ridx);
        }
        
        return mapreduceOutputName;
    }
    
    public static int getMapReduceID(Path mapreduceOutputName) {
        return getMapReduceID(mapreduceOutputName.getName());
    }
    
    public static int getMapReduceID(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(midx + 3));
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(ridx + 3));
        }
        
        return 0;
    }
    
    public static boolean isLogFiles(Path path) {
        if(path.getName().equals("_SUCCESS")) {
            return true;
        } else if(path.getName().equals("_logs")) {
            return true;
        }
        return false;
    }
    
    public static boolean isPartialOutputFiles(Path path) {
        if(path.getName().startsWith("part-r-")) {
            return true;
        } else if(path.getName().startsWith("part-m-")) {
            return true;
        }
        return false;
    }
}
