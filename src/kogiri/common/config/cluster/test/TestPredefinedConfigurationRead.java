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
package kogiri.common.config.cluster.test;

import kogiri.common.config.cluster.ClusterConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class TestPredefinedConfigurationRead {
    
    private static final Log LOG = LogFactory.getLog(TestPredefinedConfigurationRead.class);
    
    public static void main(String[] args) throws Exception {
        System.out.println("test default cluster config");
        ClusterConfiguration default_conf = ClusterConfiguration.createInstanceFromPredefined("default");
        System.out.println(default_conf.toString());
        
        System.out.println("test uits cluster config");
        ClusterConfiguration uits_conf = ClusterConfiguration.createInstanceFromPredefined("uits");
        System.out.println(uits_conf.toString());
    }
}
