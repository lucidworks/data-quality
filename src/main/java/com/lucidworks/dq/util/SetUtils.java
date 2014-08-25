package com.lucidworks.dq.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SetUtils {

  /**
   * @deprecated use {@link StringUtils#join(Collection)} instead.  
   */
  @Deprecated
  public static String join( Collection<String> strings ) {
    return StringUtils.join( strings );
  }
  /**
   * @deprecated use {@link StringUtils#join(Collection, String)} instead.  
   */
  @Deprecated
  public static String join( Collection<String> strings, String delimiter ) {
    return StringUtils.join( strings, delimiter );
  }
  /**
   * @deprecated use {@link StringUtils#splitCsv(String)} instead.  
   */
  @Deprecated  
  public static Set<String> splitCsv( String inStr ) {
    return StringUtils.splitCsv( inStr );
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

  public static <K,V> Map<K,V> mapHead( Map<K,V> inEntries, int n ) {
    if ( n < 1 ) {
      throw new IllegalStateException( "Number of desired entries must be > 0, but n = " + n );
    }
    // TODO: safe to do this?
    //if ( n >= inEntries.size() ) {
    //  return inEntries;
    //}
    Map<K,V> out = new LinkedHashMap<>();
    int counter = 0;
    for ( Entry<K, V> entry : inEntries.entrySet() ) {
      out.put( entry.getKey(), entry.getValue() );
      counter++;
      if ( counter >= n ) {
        break;
      }
    }
    // for ( int i=1; i<=n; i++ )
    return out;
  }
  public static <K,V> Map<K,V> mapTail( Map<K,V> inEntries, int n ) {
    if ( n < 1 ) {
      throw new IllegalStateException( "Number of desired entries must be > 0, but n = " + n );
    }
    List<K> keys = new ArrayList<>( inEntries.keySet() );
    List<V> values = new ArrayList<>( inEntries.values() );
    if ( keys.size() != values.size() ) {
      throw new IllegalStateException( "Number of of keys (" + keys.size() + ") != number of values (" + values.size() );
    }
    Map<K,V> out = new LinkedHashMap<>();
    int start = inEntries.size() - n - 1;
    if ( start<0 ) start = 0;
    for ( int i=start; i<keys.size(); i++ ) {
      out.put( keys.get(i), values.get(i) );
    }
    return out;
  }

  public static <K,V> Map<K,V> sortMapByValues( Map<K,V> inMap ) {
    // Inverting also sorts because we use TreeMap
    Map<V,Set<K>> invertedMap = invertMapAndSort( inMap );
    // This preserves the new order
    Map<K,V> out = uninvertMap( invertedMap );
    return out;
  }
  // using tree map for output, so automatically sorted
  public static <K,V> Map<V,Set<K>> invertMapAndSort( Map<K,V> inMap ) {
    Map<V,Set<K>> out = new TreeMap<>();
    for ( Entry<K, V> entry : inMap.entrySet() ) {
      K key = entry.getKey();
      V value = entry.getValue();
      if ( out.containsKey(value) ) {
        Set<K> vector = out.get(value);
        vector.add( key );
      }
      else {
        Set<K> vector = new TreeSet<>();
        vector.add( key );
        out.put( value, vector );
      }
    }
    return out;
  }
  // Preserve insertion order
  public static <K,V> Map<K,V> uninvertMap( Map<V,Set<K>> inMap ) {
    Map<K,V> out = new LinkedHashMap<>();
    for ( Entry<V, Set<K>> entry : inMap.entrySet() ) {
      V value = entry.getKey();
      Set<K> keys = entry.getValue();
      for ( K k : keys ) {
        if ( out.containsKey(k) ) {
          throw new IllegalArgumentException( "Duplicate entries for supposed unique key " + k );
        }
        out.put( k, value );
      }
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

  // TODO: refactor to handle anything implementing Collection
  public static boolean sameAndInSameOrder( Collection<String> idsA, Collection<String> idsB ) {
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
    Collection<String> onlyA = inAOnly_nonDestructive( idsA, idsB );
    Collection<String> onlyB = inBOnly_nonDestructive( idsA, idsB );
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
  // TODO: redo so it takes anything derived from Collection
  public static Collection<String> inAOnly_nonDestructive( Collection<String> idsA, Collection<String> idsB ) {
    Set<String> out = new LinkedHashSet<>();
    out.addAll( idsA );
    out.removeAll( idsB );
    return out;
  }
  public static Set<String> inBOnly_nonDestructive( Set<String> idsA, Set<String> idsB ) {
    return inAOnly_nonDestructive( idsB, idsA );
  }
  // TODO: redo so it takes anything derived from Collection
  public static Collection<String> inBOnly_nonDestructive( Collection<String> idsA, Collection<String> idsB ) {
    return inAOnly_nonDestructive( idsB, idsA );
  }
  public static Set<String> intersection_nonDestructive( Set<String> idsA, Set<String> idsB ) {
    Set<String> out = new LinkedHashSet<>();
    out.addAll( idsA );
    out.retainAll( idsB );
    return out;
  }
  public static Collection<String> intersection_nonDestructive( Collection<String> idsA, Collection<String> idsB ) {
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