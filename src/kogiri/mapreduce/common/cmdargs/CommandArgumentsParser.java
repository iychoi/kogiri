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
package kogiri.mapreduce.common.cmdargs;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class CommandArgumentsParser<T extends CommandArgumentsBase> {
    public boolean parse(String[] args, T cmdargs) {
        CmdLineParser parser = new CmdLineParser(cmdargs);
        CmdLineException parseException = null;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            parseException = e;
        }
        
        if(cmdargs.isHelp() || !cmdargs.checkValidity()) {
            parser.printUsage(System.err);
            return false;
        }
        
        if(parseException != null) {
            System.err.println(parseException.getMessage());
            parser.printUsage(System.err);
            return false;
        }
        return true;
    }
}
