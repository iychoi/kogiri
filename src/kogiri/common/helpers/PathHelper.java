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
package kogiri.common.helpers;

/**
 *
 * @author iychoi
 */
public class PathHelper {
    public static String getParent(String path) {
        // check root
        if(path.equals("/")) {
            return null;
        }
        
        int lastIdx = path.lastIndexOf("/");
        if(lastIdx > 0) {
            return path.substring(0, lastIdx);
        } else {
            return "/";
        }
    }
    
    public static String concatPath(String path1, String path2) {
        StringBuffer sb = new StringBuffer();
        
        if(path1 != null && !path1.isEmpty()) {
            sb.append(path1);
        }
        
        if(!path1.endsWith("/")) {
            sb.append("/");
        }
        
        if(path2 != null && !path2.isEmpty()) {
            if(path2.startsWith("/")) {
                sb.append(path2.substring(1, path2.length()));
            } else {
                sb.append(path2);
            }
        }
        
        return sb.toString();
    }
}
