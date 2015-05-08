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

import java.io.IOException;
import kogiri.Kogiri;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

/**
 *
 * @author iychoi
 */
public class MapReduceClusterHelper {
    
    private static final Log LOG = LogFactory.getLog(Kogiri.class);
    
    public static int getNodeNum() {
        try {
            JobClient client = new JobClient();
            ClusterStatus clusterStatus = client.getClusterStatus();
            return clusterStatus.getTaskTrackers();
        } catch (IOException ex) {
            LOG.error(ex);
            return -1;
        }
    }
}
