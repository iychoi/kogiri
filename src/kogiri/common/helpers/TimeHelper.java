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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author iychoi
 */
public class TimeHelper {
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }
    
    public static String getDiffTimeString(long begin, long end) {
        long diff = end - begin;
        long remain = diff;
        
        int msec = (int) (remain % 1000);
        remain /= 1000;
        int sec = (int) (remain % 60);
        remain /= 60;
        int min = (int) (remain % 60);
        remain /= 60;
        int hour = (int) (remain);
        
        return hour + "h " + min + "m " + sec + "s";
    }
    
    public static String getTimeString(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
