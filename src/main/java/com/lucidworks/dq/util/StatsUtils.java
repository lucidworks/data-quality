package com.lucidworks.dq.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class StatsUtils {

  // Quick Stats
  // For ints, we still return long as a sum
  public static long sumList_Ints( Collection<Integer> in ) {
	long out = 0L;
	for ( Integer i : in ) {
      if ( null!=i ) {
        out += i;
	  }
	}
	return out;
  }
  public static long sumList_Longs( Collection<Long> in ) {
	long out = 0L;
	for ( Long i : in ) {
      if ( null!=i ) {
		out += i;
      }
	}
	return out;
  }
  public static double sumList_Doubles( Collection<Double> in ) {
	double out = 0L;
	for ( Double i : in ) {
	  if ( null!=i ) {
        out += i;
	  }
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
  public static double averageList_Doubles( Collection<Double> in ) {
	if ( null==in || in.isEmpty() ) {
	  return Double.NaN;
	}
	Double sum = sumList_Doubles( in );
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

  public static List<Double> longs2Doubles( Collection<Long> longs ) {
	List<Double> out = new ArrayList<>();
	for ( Long l : longs ) {
	  Double d = new Double(l);
	  out.add(  new Double( l )  );
	}
	return out;
  }

  // http://math.stackexchange.com/questions/350754/fitting-exponential-curve-to-data
  // Returns [A,k] for y = A e^kx
  public static double [] leastSquares_Exponential( List<Double> xList, List<Double> yList ) {
	List<Double> xList2 = new ArrayList<>();
	List<Double> yList2 = new ArrayList<>();
	// Skip zeros!
	for ( int i=0; i<xList.size(); i++ ) {
	  Double y = yList.get(i);
	  if ( y > 0.0 ) {
		Double x = xList.get(i);
		double y2 = Math.log(y);
		xList2.add( x );
		yList2.add( y2 );
	  }
	  // yList2.add( Math.log(d) );
	}
	double [] line = leastSquares_Line( xList2, yList2 );
	double m = line[0];
	double b = line[1];
    double A = Math.exp( b );
    double k = m;
    double out[] = new double[2];
    out[0] = A;
    out[1] = k;
    System.out.println( "leastSquares_Exponential: returning [A, k] = [" + A + ", " + k + "]" );
    return out;
  }
  // Retruns [m, b] for y = mx+b
  // http://hotmath.com/hotmath_help/topics/line-of-best-fit.html
  public static double [] leastSquares_Line( List<Double> xList, List<Double> yList ) {
    double m = 0;
    double b = 0;
	if ( xList.size() != yList.size() ) {
      throw new IllegalStateException( "Number of of x values (" + xList.size() + ") != number of y (" + yList.size() );
	}
	if ( xList.size() > 0 ) {
	  double sumX = 0;
	  double sumY = 0;
	  double sumXY = 0;
	  double sumSquaredX = 0;
	  for ( int i=0; i<xList.size(); i++ ) {
		double x = xList.get(i);
		double y = yList.get(i);
		sumX += x;
		sumY += y;
		sumXY += x * y;
		sumSquaredX += x * x;
	  }
	  double meanX = averageList_Doubles( xList );
	  double meanY = averageList_Doubles( yList );
	  double n = (double) xList.size();
	  double m_mumerator = sumXY - sumX*sumY / n;
	  double m_denominator = sumSquaredX - sumX*sumX / n;
	  m = m_mumerator / m_denominator;
	  b = meanY - m * meanX;
	  System.out.println( "leastSquares_Line: x["+xList.size()+"] = " + xList );
	  System.out.println( "leastSquares_Line: y["+xList.size()+"] = " + yList );
	  System.out.println( "leastSquares_Line: sumX = " + sumX );
	  System.out.println( "leastSquares_Line: sumY = " + sumY );
	  System.out.println( "leastSquares_Line: sumXY = " + sumXY );
	  System.out.println( "leastSquares_Line: sumSquaredX = " + sumSquaredX );
	  System.out.println( "leastSquares_Line: meanX = " + meanX );
	  System.out.println( "leastSquares_Line: meanY = " + meanY );
	  System.out.println( "leastSquares_Line: n = " + n );
	  System.out.println( "leastSquares_Line: m_mumerator = " + m_mumerator );
	  System.out.println( "leastSquares_Line: m_denominator = " + m_denominator );
	    System.out.println( "leastSquares_Line: returning [m, b] = [" + m + ", " + b + "]" );
	}
	else {
	  System.err.println( "Warn: leastSquares_Line: no values, nothing to do, returning zeros" );
	}
    double out[] = new double[2];
    out[0] = m;
    out[1] = b;
    return out;
  }
}