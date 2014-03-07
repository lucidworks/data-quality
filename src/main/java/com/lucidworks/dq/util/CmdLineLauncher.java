package com.lucidworks.dq.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CmdLineLauncher {
  // TODO: currently using static init but
  // fefactoring would require that all classes use lightweight null constructor
  // static final Map<String, Class<? extends HasDescription>> CLASSES = new LinkedHashMap<String,Class<? extends HasDescription>>()
  static final Map<String, Class<?>> CLASSES = new LinkedHashMap<String,Class<?>>()
  {{
    put( "empty_fields",      com.lucidworks.dq.data.EmptyFieldStats.class     );
    put( "term_stats",        com.lucidworks.dq.data.TermStats.class           );
    put( "code_points",       com.lucidworks.dq.data.TermCodepointStats.class  );
    put( "data_checker",      com.lucidworks.dq.data.DateChecker.class         );
    put( "diff_empty_fields", com.lucidworks.dq.diff.DiffEmptyFieldStats.class );
    put( "diff_ids",          com.lucidworks.dq.diff.DiffIds.class             );
    put( "diff_schema",       com.lucidworks.dq.diff.DiffSchema.class          );
  }};
  public static void main( String[] argv ) {
    if( argv.length < 1 ) {
      System.out.println( "Pass a command name on the command line to see help for that class:" );
      // for( Entry<String, Class<? extends HasDescription>> entry : CLASSES.entrySet() )
      for( Entry<String, Class<?>> entry : CLASSES.entrySet() )
      {
        String cmdName = entry.getKey();
        // Class<? extends HasDescription> clazz = entry.getValue();
        Class<?> clazz = entry.getValue();

        String desc = null;
        try {
			Method descMeth = clazz.getMethod( "getShortDescription" );
			desc = (String) descMeth.invoke( null, (Object[]) null );
        	// Field f = clazz.getDeclaredField( "HELP_WHAT_IS_IT" );
        	// desc = (String) f.get(null);
		} catch (SecurityException | IllegalArgumentException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // System.out.println( cmdName + ": " + desc );
        System.out.printf( "%20s: %s\n", cmdName, desc );
      }
    }
    // Has a command name
    else {
      String cmdName = argv[ 0 ];
      if ( CLASSES.containsKey(cmdName) ) {
        // Copy over all the first arg
        String [] argv2 = new String[ argv.length - 1 ];
        for ( int i=1; i<argv.length; i++ ) {
          argv2[ i-1 ] = argv[ i ];
        }
        Class<?> clazz = CLASSES.get(cmdName);
        try {
          Method main = clazz.getMethod( "main", String[].class );
          // main.invoke( null, argv2 );
          // main.invoke( null, (Object[]) argv2 );
          main.invoke( null, (Object) argv2 );
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          System.exit(2);
        }
      }
      else {
        System.err.println( "Command \"" + cmdName + "\" not found in " + CLASSES.keySet() );
        System.exit(2);
      }
    }
  }
}