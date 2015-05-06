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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * @author iychoi
 */
public class ClassHelper {
    public static Class findClass(String className, String[] lookupPaths) throws ClassNotFoundException {
        if(className == null || className.isEmpty()) {
            throw new IllegalArgumentException("className is not given");
        }
        
        Class clazz = null;
        
        // check whether the given className is full package path
        try {
            clazz = Class.forName(className);
        } catch(ClassNotFoundException ex) {
        }

        // if the given className is not a full package path
        if(clazz == null) {
            for(String pkg : lookupPaths) {
                String newClassName = pkg + "." + className;
                try {
                    clazz = Class.forName(newClassName);
                } catch(ClassNotFoundException ex) {
                }

                if(clazz != null) {
                    break;
                }
            }
        }

        if(clazz == null) {
            throw new ClassNotFoundException("Class was not found : " + className);
        }
        
        return clazz;
    }
    
    public static Object getClassInstance(Class clazz) throws InstantiationException, IllegalAccessException {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        
        return clazz.newInstance();
    }
    
    public static void invokeMain(Class clazz, String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        
        Method method = null;

        try {
            // find main function
            Class[] argTypes = new Class[] { String[].class };
            method = clazz.getDeclaredMethod("main", argTypes);
        } catch(NoSuchMethodException ex) {
            throw new NoSuchMethodException("main function was not found in " + clazz.getName() + " class");
        }
        try {
            method.invoke(null, (Object)args);
        } catch (IllegalAccessException ex) {
            throw ex;
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (InvocationTargetException ex) {
            throw ex;
        }
    }
}
