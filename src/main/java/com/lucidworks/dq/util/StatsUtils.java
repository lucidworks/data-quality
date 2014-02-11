package com.lucidworks.dq.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class StatsUtils {

  // Quick Stats
  // For ints, we still return long as a sum
  public static long sumList_Ints( Collection<Integer> in ) {
	long out = 0L;
	for ( Integer i : in ) {
      out += i;
	}
	return out;
  }
  public static long sumList_Longs( Collection<Long> in ) {
	long out = 0L;
	for ( Long i : in ) {
      out += i;
	}
	return out;
  }
  public static int minList_Ints( Collection<Integer> in ) {
	int out = Integer.MAX_VALUE;
	for ( Integer i : in ) {
	  if ( i < out ) {
		out = i;
	  }
	}
	return out;
  }
  public static long minList_Longs( Collection<Long> in ) {
	long out = Long.MAX_VALUE;
	for ( Long i : in ) {
	  if ( i < out ) {
		out = i;
	  }
	}
	return out;
  }
  public static int maxList_Ints( Collection<Integer> in ) {
	int out = Integer.MIN_VALUE;
	for ( Integer i : in ) {
	  if ( i > out ) {
		out = i;
	  }
	}
	return out;
  }
  public static long maxList_Longs( Collection<Long> in ) {
	long out = Long.MIN_VALUE;
	for ( Long i : in ) {
	  if ( i > out ) {
		out = i;
	  }
	}
	return out;
  }
  public static double averageList_Ints( Collection<Integer> in ) {
	if ( null==in || in.isEmpty() ) {
	  return Double.NaN;
	}
	Long sum = sumList_Ints( in );
	return (double) sum / (double) in.size();
	// return new Double(sum) / new Double(in.size());
  }
  public static double averageList_Longs( Collection<Long> in ) {
	if ( null==in || in.isEmpty() ) {
	  return Double.NaN;
	}
	Long sum = sumList_Longs( in );
	return (double) sum / (double) in.size();
	// return new Double(sum) / new Double(in.size());
  }
  // TODO: assumes full-population std, could add flag for sample, N-1 logic
  public static double standardDeviationList_Ints( Collection<Integer> in ) {
	if ( null==in || in.isEmpty() ) {
	  return 0.0;
	}
	double avg = averageList_Ints( in );
	double sumOfDeltaSquared = 0.0;
	for ( int i : in ) {
      // Order doesn't matter since we square it
	  double delta = avg - (double)i;
	  sumOfDeltaSquared += delta * delta;
	}
	return Math.sqrt( sumOfDeltaSquared / (double)in.size() );
  }
  public static double standardDeviationList_Longs( Collection<Long> in ) {
	if ( null==in || in.isEmpty() ) {
	  return 0.0;
	}
	double avg = averageList_Longs( in );
	double sumOfDeltaSquared = 0.0;
	for ( long i : in ) {
      // Order doesn't matter since we square it
	  double delta = avg - (double) i;
	  sumOfDeltaSquared += delta * delta;
	}
	return Math.sqrt( sumOfDeltaSquared / (double)in.size() );
  }
	
}