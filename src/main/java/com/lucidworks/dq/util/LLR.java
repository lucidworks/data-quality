package com.lucidworks.dq.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class LLR {

  Map<String,Long> wordsA;
  Map<String,Long> wordsB;
  // TODO: consider this, BUT threshold for A OR B, or A AND B ?
  // long minWordsThreshold = 0L;

  // Column Totals
  double sumA = 0.0;
  double sumB = 0.0;
  // K Total
  double grandTotal;
  // Row Totals
  Map<String,Double> rowTotals = new LinkedHashMap<>();

  // Set<String> allWordsAboveThreshold = new TreeSet<>();
  Set<String> allWords = new TreeSet<>();

  Map<String,Double> scoresByWord = new TreeMap<>();
  Map<String,Double> sortedScoresByWord = new TreeMap<>();

  // Peformance Stat
  long plogp_counter = 0L;

  public LLR( Map<String,Long> wordsA, Map<String,Long> wordsB /*, Long optThreshold*/ ) {
	this.wordsA = wordsA;
	this.wordsB = wordsB;
	//if ( null!=optThreshold && optThreshold.longValue() > 0L ) {
	//  this.minWordsThreshold = optThreshold.longValue();
	//}
	doInitialCalculations();
	calcAllWords();
	sortWords();
  }

  public void doInitialCalculations() {

    // Column Totals
    // -------------
    // sumA = sumWithThreshold( wordsA.values() );
    // sumB = sumWithThreshold( wordsB.values() );
	sumA = new Double( StatsUtils.sumList_Longs(wordsA.values()) ).doubleValue();
	sumB = new Double( StatsUtils.sumList_Longs(wordsB.values()) ).doubleValue();
	if ( sumA<=0.0 || sumB<=0.0 ) {
      throw new IllegalArgumentException( "Must have non-zero word counts: A=" + sumA + ", B=" + sumB );
	}

    // K Total
    grandTotal = sumA + sumB;

    // Row Totals
    // ----------
    allWords.addAll( wordsA.keySet() );
    allWords.addAll( wordsB.keySet() );
    for ( String word : allWords ) {
      Long countA = wordsA.containsKey(word) ? wordsA.get(word) : 0L;
      Long countB = wordsB.containsKey(word) ? wordsB.get(word) : 0L;
      rowTotals.put( word, new Double(countA + countB) );
    }

  }

  public void calcAllWords() {
	for ( String word : allWords ) {
	  // double g2 = calcG2_viaDunning( word );
	  double g2 = calcG2_viaTraditional( word );
	  scoresByWord.put( word, g2 );
    }
  }


  // TODO: G2 is the same as -2 log lambda ?
  // http://scg.unibe.ch/archive/papers/Kuhn09aLogLikelihoodRatio.pdf
  // Before Sign:
  //   food: 0.0
  //   bananas: 0.46192170199964266
  //   apples: 0.6291706616789554
  //   carrots: 60.03320678316349
  //   candy: 60.03320678316351
  // After Sign:
  //  candy: -60.03320678316351
  //  bananas: -0.46192170199964266
  //  food: 0.0
  //  apples: 0.6291706616789554
  //  carrots: 60.03320678316349
  double calcG2_viaTraditional( String word ) {
    boolean debug = false;
    if(debug) System.out.println( "\n=== Calculating G2 via Traditional formula for \"" + word + "\" ===" );
    // Simple terms
    double k1 = wordsA.containsKey(word) ? wordsA.get(word) : 0L;
    double k2 = wordsB.containsKey(word) ? wordsB.get(word) : 0L;
    double n1 = sumA;
    double n2 = sumB;
    double p1 = k1 / n1;
    double p2 = k2 / n2;
    if(debug) System.out.println( "Corpus A: k1, n1, p1: " + k1 + ", " + n1 + ", " + p1 );
    if(debug) System.out.println( "Corpus B: k2, n2, p2: " + k2 + ", " + n2 + ", " + p2 );
    double p = (k1 + k2) / (n1 + n2);  // rowCount / grandTotal
    if(debug) System.out.println( "Combined: k1+2, n1+2, p1+2: " + (k1+k2) + ", " + (n1+n2) + ", " + p );
    // Factors
    double factorA = Math.log( L(p1,k1,n1) );
    double factorB = Math.log( L(p2,k2,n2) );
    double factorC = Math.log(  L(p,k1,n1) );
    double factorD = Math.log(  L(p,k2,n2) );
    double sign = sign( p1, p2 );
    // Result
    double out = sign * 2.0 * ( factorA + factorB - factorC - factorD );
    if(debug) System.out.println( "out = +/-sign * 2.0 * ( factorA + factorB - factorC - factorD )" );
    if(debug) System.out.println( "Sign and Factors A, B, C, D: " + sign + ", " + factorA + ", " + factorB + ", " + factorC + ", " + factorD );
    if(debug) System.out.println( "out = " + out );
    return out;
  }

  // TODO: this is Binomial Likelihood ?
  // k = word count
  // n = total words in corpus (non-unique)
  // p = k/n, BUT might use different k and n
  static double L( double p, double k, double n ) {
	double part1 = Math.pow( p, k );
	double part2 = Math.pow( (1.0-p), (n-k) );
	return part1 * part2;
  }

  // TODO: confirm meaning of +/-
  // plus = heavier in first collection
  // minus = heavier in second collection
  static double sign( double p1, double p2 ) {
    if ( p1 - p2 >= 0.0 ) {
      return 1.0;
    }
    else {
      return -1.0;
    }
  }

  // Each word is done individually, across both collections
  //   food: 1.7319479184152442E-13
  //   bananas: 0.4619217019995059
  //   apples: 0.6291706616789394
  //   candy: 60.03320678316341
  //   carrots: 60.03320678316341
  double calcG2_viaDunning( String word ) {
    boolean debug = false;
    if(debug) System.out.println( "\n=== Calculating G2 via Dunning Entropy formula for \"" + word + "\" ===" );
    // Calc H_rowSums
	// ---------------
    double row1Total = rowTotals.get(word);
    double row2Total = grandTotal - row1Total;
    if(debug) System.out.println( "Row Totals: " + row1Total + "  " + row2Total );
    // plnp = probability * log (probability), log = natural log
    double plogpRow1 = 0.0;
    if ( row1Total > 0.0 ) {
      double prob = row1Total / grandTotal;
      plogpRow1 = prob * Math.log(prob);
      plogp_counter++;
    }
    double plogpRow2 = 0.0;
    if ( row2Total > 0.0 ) {
      double prob = row2Total / grandTotal;
      plogpRow2 = prob * Math.log(prob);
      plogp_counter++;
    }
    double H_rowSums = -1.0 * ( plogpRow1 + plogpRow2 );
    if(debug) System.out.println( "Row plogp 1 & 2 and H_rowSums: " + plogpRow1 + "  " + plogpRow2 + "  " + H_rowSums );

    // Calc H_colSums
    // --------------
    // We checked column sums earlier
    double probCol1 = sumA / grandTotal;
    double plogpCol1 = probCol1 * Math.log( probCol1 );
    plogp_counter++;
    double probCol2 = sumB / grandTotal;
    double plogpCol2 = probCol2 * Math.log( probCol2 );
    plogp_counter++;
    double H_colSums = -1.0 * ( plogpCol1 + plogpCol2 );
    if(debug) System.out.println( "Column plogp 1 & 2 and H_colSums: " + plogpCol1 + "  " + plogpCol2 + "  " + H_colSums );

    // Calc H_k
    // -----------
    // column 1 counts
    double k_11 = wordsA.containsKey(word) ? wordsA.get(word) : 0L;
    double k_21 = sumA - k_11;  // all other counts
    // column 2 counts
    double k_12 = wordsB.containsKey(word) ? wordsB.get(word) : 0L;
    double k_22 = sumB - k_12;  // all other counts
    if(debug) System.out.println( "K counts:\n\t" + k_11 + "  " + k_12 + "\n\t" + k_21 + "  " + k_22 );
    // probabilities
    double prob_11 = k_11 / grandTotal;
    double prob_21 = k_21 / grandTotal;
    double prob_12 = k_12 / grandTotal;
    double prob_22 = k_22 / grandTotal;
    // p log( p )
    // method has its own counter
    double plogp_11 = plogp( prob_11 );
    double plogp_21 = plogp( prob_21 );
    double plogp_12 = plogp( prob_12 );
    double plogp_22 = plogp( prob_22 );
    // finally H_k
    double H_k = -1.0 * ( plogp_11 + plogp_21 + plogp_12 + plogp_22 );
    if(debug) System.out.println( "K plogp:\n\t" + plogp_11 + "  " + plogp_12 + "\n\t" + plogp_21 + "  " + plogp_22 );
    if(debug) System.out.println( "H_k = " + H_k );

    // Dunning's formula
    // http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html
//    double G2 = 2.0 * grandTotal * ( H_k - H_rowSums - H_colSums );
//    if(debug) System.out.println( "G2 = 2.0 * grandTotal * ( H_k - H_rowSums - H_colSums )" );
//    if(debug) System.out.println( "2 * " + grandTotal + " * ( " + H_k + " - " + H_rowSums + " - " + H_colSums + " )" );

    // Revised, see http://math.stackexchange.com/questions/693114/wrong-result-from-llr-using-dunning-entropy-method
    double G2 = 2.0 * grandTotal * ( H_rowSums + H_colSums - H_k );
    if(debug) System.out.println( "G2 = 2.0 * grandTotal * ( H_rowSums + H_colSums - H_k )" );
    if(debug) System.out.println( "2 * " + grandTotal + " * ( " + H_rowSums + " + " + H_colSums + " - " + H_k + " )" );

    return G2;
  }

  // Calculates p * log( p )
  // natural log
  // but returns 0.0 if p is 0
  // TODO: maybe some implementitons just add 1 to all counts?
  double plogp( double prob ) {
	if ( prob > 0.0 ) {
	  plogp_counter++;
	  return prob * Math.log( prob );
	}
	else {
	  return 0.0;
	}
  }

  void sortWords() {
	//  Map<String,Double> scoresByWord = new TreeMap<>();
	//  Map<String,Double> sortedScoresByWord = new TreeMap<>();
    sortedScoresByWord = SetUtils.sortMapByValues( scoresByWord );
  }

//  double pLogP_KOverallWordA( String word ) {
//	double prob = probKOverallWordA( word );
//	if ( prob > 0.0 ) {
//      return prob * Math.log( prob );
//	}
//	else {
//	  return 0.0;
//	}
//  }
//  double pLogP_KOverallWordB( String word ) {
//	double prob = probKOverallWordB( word );
//	if ( prob > 0.0 ) {
//      return prob * Math.log( prob );
//	}
//	else {
//	  return 0.0;
//	}
//  }
//  double probKOverallWordA( String word ) {
//	return probKOverallWord( word, wordsA );
//  }
//  double probKOverallWordB( String word ) {
//    return probKOverallWord( word, wordsB );
//  }
//  double probKOverallWord( String word, Map<String,Long> countMap ) {
//    long count = countMap.containsKey(word) ? countMap.get(word) : 0L;
//    double prob = (double) count / grandTotal;
//    return prob;
//  }

//  double sumWithThreshold( Collection<Long> counts ) {
//    double out = 0.0;
//	for ( Long c : counts ) {
//      if ( c >= minWordsThreshold ) {
//    	out += c;
//      }
//    }
//    return out;
//  }

  public String generateReport( String optLabel ) {
	StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    int sampleSize = 5;

    if ( null!=optLabel ) {
    	out.println( "----------- " + optLabel + " -----------" );
    }

    out.println();
    out.println( "Corpus A unique / total words: " + wordsA.size() + " / " + sumA );
    out.println( "Corpus B unique / total words: " + wordsB.size() + " / " + sumB );
    out.println( "Combined unique / total words: " + allWords.size() + " / " + grandTotal );
    out.println( "Number of p log(p) calculations: " + plogp_counter );
    out.println();

    if ( sortedScoresByWord.size() <= 2 * sampleSize + 1 ) {
      addTermsSliceToReport( out, "All Term Changes", sortedScoresByWord );
    }
    else {
      Map<String,Double> firstTerms = SetUtils.mapHead( sortedScoresByWord, sampleSize );
      addTermsSliceToReport( out, "Term Changes, first " + sampleSize + " entries", firstTerms );
      Map<String,Double> lastTerms = SetUtils.mapTail( sortedScoresByWord, sampleSize );
      addTermsSliceToReport( out, "Term Changes, last " + sampleSize + " entries", lastTerms );
    }

    String outStr = sw.toString();
    return outStr;
  }
  void addTermsSliceToReport( PrintWriter out, String label, Map<String,Double> terms ) {
    out.println( "" + label + ":" );
    for ( Entry<String, Double> wordEntry : terms.entrySet() ) {
      String word = wordEntry.getKey();
      double g2 = wordEntry.getValue();
      out.println( "\t" + word + ": " + g2 );
    }  
  }
  
  public static void main( String[] argv ) throws SolrServerException {
//	Map<String,Long> corpusA = new LinkedHashMap<String,Long>() {{
//      // 100k docs total
//	  put( "blog",        25L );  // test word
//	  put( "computer",  3200L );  // other words
//	  put( "internet", 96775L );  // other words
//	}};
//	Map<String,Long> corpusB = new LinkedHashMap<String,Long>() {{
//      // 200k docs total
//      put( "blog",       2500L ); // test word
//      put( "computer",   6000L ); // other words
//      put( "internet", 191500L ); // other words
//    }};

//    // Example posted online
//	Map<String,Long> corpusA = new LinkedHashMap<String,Long>() {{
//	  // 100k docs total
//      put( "spam",        40000L );  // test word
//      put( "other words", 60000L );  // other words
//    }};
//    Map<String,Long> corpusB = new LinkedHashMap<String,Long>() {{
//      // 200k docs total
//      put( "spam",        120000L ); // test word
//      put( "other words",  80000L ); // other words
//    }};

  Map<String,Long> corpusA = new LinkedHashMap<String,Long>() {{
    put( "apples",   25L );
    put( "bananas",  30L );
    put( "carrots",  40L );
    put( "food",    100L );
  }};
  Map<String,Long> corpusB = new LinkedHashMap<String,Long>() {{
    put( "apples",   20L ); // down by 5
    put( "bananas",  35L ); // up by 5
    put( "candy",    40L ); // carrots -> candy!
    put( "food",    100L ); // unchanged, and total unchanged
  }};


//    HttpSolrServer solrA = SolrUtils.getServer( "localhost", 8984 );   
//    HttpSolrServer solrB = SolrUtils.getServer( "localhost", 8985 );
//    String fieldName = "text";
//    // Set<String> corpusA = SolrUtils.getTermsForField_ViaTermsRequest( solrA, fieldName );
//    // Set<String> corpusB = SolrUtils.getTermsForField_ViaTermsRequest( solrB, fieldName );
//    Map<String, Long> corpusA = SolrUtils.getAllTermsAndCountsForField_ViaTermsRequest( solrA, fieldName );
//    Map<String, Long> corpusB = SolrUtils.getAllTermsAndCountsForField_ViaTermsRequest( solrB, fieldName );

	LLR llr = new LLR( corpusA, corpusB );
    String report = llr.generateReport( "A -> B" );
    System.out.print( report );

  }
}