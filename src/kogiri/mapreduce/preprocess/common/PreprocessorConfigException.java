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
package kogiri.mapreduce.preprocess.common;

/**
 *
 * @author iychoi
 */
public class PreprocessorConfigException extends Exception {
    static final long serialVersionUID = 7818375828146090155L;

    public PreprocessorConfigException() {
        super();
    }

    public PreprocessorConfigException(String string) {
        super(string);
    }

    public PreprocessorConfigException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public PreprocessorConfigException(Throwable thrwbl) {
        super(thrwbl);
    }
}
