package com.lucidworks.dq.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SetUtils {

  public static String join( Collection<String> strings ) {
    return join( strings, ", " );
  }
  public static String join( Collection<String> strings, String delimiter ) {
	StringBuffer out = new StringBuffer();
	boolean isFirst = true;
	for ( String s : strings ) {
	  if ( ! isFirst ) {
		out.append( delimiter );
	  }
	  else {
		isFirst = false;
	  }
	  out.append( s );
	}
	return new String( out );
  }
  
  // Handy for comma separated field names list, etc
  public static Set<String> splitCsv( String inStr ) {
	String[] fieldsAry = inStr.split( ",\\s*" );
	// maintains order of insertion
	Set<String> out = new LinkedHashSet<>();
	for ( String f : fieldsAry ) {
	  if ( ! f.trim().isEmpty() ) {
		out.add( f.trim() );
	  }
	}
	return out;
  }

  // Assumes always using LinkedHashMap which keep things in predictable insertion order
  public static <K,V> Map<K,V> reverseMapEntryKeyOrder( Map<K,V> inEntries ) {
	List<K> keys = new ArrayList<>( inEntries.keySet() );
	List<V> values = new ArrayList<>( inEntries.values() );
	if ( keys.size() != values.size() ) {
	  throw new IllegalStateException( "Number of of keys (" + keys.size() + ") != number of values (" + values.size() );
	}
	Map<K,V> out = new LinkedHashMap<>();
	for ( int i=keys.size()-1; i>=0; i-- ) {
	  out.put( keys.get(i), values.get(i) );
	}
	return out;
  }

  public static boolean sameAndInSameOrder( Set<String> idsA, Set<String> idsB ) {
	  // Bunch of edge cases
	  // TODO: maybe move edge cases to same set
	  // TODO: other methods don't do null checking....
	  if ( null==idsA && null==idsB ) {
		  return true;
	  }
	  if ( null==idsA ) {
		  return null==idsB || idsB.isEmpty();
	  }
	  if ( null==idsB ) {
		  return null==idsA || idsA.isEmpty();
	  }
	  if ( idsA.isEmpty() && idsB.isEmpty() ) {
		  return true;
	  }
	  if ( idsA.size() != idsB.size() ) {
		  return false;
	  }
	  Set<String> onlyA = inAOnly_nonDestructive( idsA, idsB );
	  Set<String> onlyB = inBOnly_nonDestructive( idsA, idsB );
	  if ( ! onlyA.isEmpty() || ! onlyB.isEmpty() ) {
		  return false;
	  }
	  // OK, walk them together
	  // And we've checked the sizes
	  Iterator<String> itA = idsA.iterator();
	  Iterator<String> itB = idsB.iterator();
	  
	  // Note:
	  // The while and if checks look redundant
	  // but they handle the very unlikely edge case
	  // where one list is added to while we're looping
	  // and gets longer - that means FALSE
	  // but if loop just ended we'd accidently return true
	  while ( itA.hasNext() || itB.hasNext() ) {
		  if ( ! itA.hasNext() || ! itB.hasNext() ) {
			  return false;
		  }
		  String itemA = itA.next();
		  String itemB = itB.next();
		  if ( ! itemA.equals(itemB) ) {
			  return false;
		  }
	  }
	  // All tests have passed
	  return true;
  }
  
  // Non-Destructive

  public static Set<String> inAOnly_nonDestructive( Set<String> idsA, Set<String> idsB ) {
	  Set<String> out = new LinkedHashSet<>();
	  out.addAll( idsA );
	  out.removeAll( idsB );
	  return out;
  }
  public static Set<String> inBOnly_nonDestructive( Set<String> idsA, Set<String> idsB ) {
	  return inAOnly_nonDestructive( idsB, idsA );
  }
  public static Set<String> intersection_nonDestructive( Set<String> idsA, Set<String> idsB ) {
	  Set<String> out = new LinkedHashSet<>();
	  out.addAll( idsA );
	  out.retainAll( idsB );
	  return out;
  }
  public static Set<String> union_nonDestructive( Set<String> idsA, Set<String> idsB ) {
	  Set<String> out = new LinkedHashSet<>();
	  out.addAll( idsA );
	  out.addAll( idsB );
	  return out;
  }
  
  // Destructive

  public static Set<String> inAOnly_destructive( Set<String> idsA, Set<String> idsB ) {
	  idsA.removeAll( idsB );
	  return idsA;
  }
  public static Set<String> inBOnly_destructive( Set<String> idsA, Set<String> idsB ) {
	  return inAOnly_destructive( idsB, idsA );
  }
  public static Set<String> intersection_destructive( Set<String> idsA, Set<String> idsB ) {
	  idsA.retainAll( idsB );
	  return idsB;
  }
  public static Set<String> union_destructive( Set<String> idsA, Set<String> idsB ) {
	  idsA.addAll( idsB );
	  return idsA;
  }
	
}