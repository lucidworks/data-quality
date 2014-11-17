package com.lucidworks.dq.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TupleEntropy {
  static boolean debug = false;
  public static double calcTupleEntropyForAllLengths( String word ) {
    double outEntropy = 0.0;
    for ( int i=1; i<=word.length(); i++ ) {
      double newEntropy = calcTupleEntropyForLength( word, i );
      outEntropy += newEntropy;
    }
    return outEntropy;
  }
  public static double calcTupleEntropyForLength( String word, int len ) {
    Map<String,Double> tupleStats = calcTuplesForLen( word, len );
    double outEntropy = calcEntropyForCounts( tupleStats );
    if(debug) System.out.println( "\tTuple Len: " + len + " has " + tupleStats.keySet().size() + " / " + StatsUtils.sumList_Doubles(tupleStats.values())  + " unique/total" );
    if(debug) System.out.println( "\t\tTuples: " + tupleStats );
    if(debug) System.out.println( "\t\tEntropy = " + outEntropy );
    return outEntropy;
  }
  public static double calcEntropyForCounts( Map<String,Double> inMap ) {
    if ( null==inMap || inMap.isEmpty() ) {
      return 0.0;
    }
    double sum = StatsUtils.sumList_Doubles( inMap.values() );
    if ( sum <= 0.0 ) {
      return 0.0;
    }
    double outEntropy = 0.0;
    for ( Entry<String, Double> entry : inMap.entrySet() ) {
      String word = entry.getKey();
      double count = entry.getValue();
      double prob = count / sum;
      if ( prob > 0.0 ) {
        double newEntropy = -1.0 * prob * Math.log( prob );
        outEntropy += newEntropy;
      }
    }
    return outEntropy;
  }
  public static Map<String,Double> calcTuplesForLen( String word, int len ) {
    Map<String,Double> out = new LinkedHashMap<>();
    if ( len > 0 && word.length() >= len ) {
      for ( int i=0; i <= word.length() - len; i++ ) {
        String tupe = word.substring( i, i + len );
        double oldCount = 0.0;
        if ( out.containsKey(tupe) ) {
          oldCount = out.get( tupe );
        }
        out.put( tupe, oldCount + 1.0 );
      }
    }
    return out;
  }
  public static void main( String[] argv ) {
    for ( String word : argv ) {
      if(debug) System.out.println( "Word: \"" + word + "\"" );
      double entropy = calcTupleEntropyForAllLengths( word );
      if(debug) System.out.println( "\ttotal for word \"" + word + "\": " + entropy );
      if ( ! debug ) {
        System.out.println( "" + word + "\t" + word.length() + "\t" + entropy );
      }
    }
  }
}