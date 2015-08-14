package com.lucidworks.dq.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.cloud.DocRouter.Range;
import org.apache.solr.common.util.Hash;

public class HashAndShard {
  
  // Should correspond to:
  // http://localhost:8983/solr/collection1/select?q=*&fl=*,[shard]

  static String HELP_WHAT_IS_IT = "Calculate hash and shard for a document ID";
  static String HELP_USAGE = "HashAndShard docId [numberOfShards [-q]]    # shards can be decimal, hex, octal, etc";
  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }


  /* From:
   * solr-lucene-490-src/solr/solrj/src/java/org/apache/solr/common/cloud/CompositeIdRouter.java
   */
  private static int bits = 16;
  static List<Range> partitionRange( int partitions ) {
    int min = Integer.MIN_VALUE; // -2^31   = -2147483648 = -2,147,483,648
    int max = Integer.MAX_VALUE; //  2^31-1 =  2147483647 =  2,147,483,647

    // assert max >= min;
    // if (partitions == 0) return Collections.EMPTY_LIST;
    long rangeSize = (long) max - (long) min;
    long rangeStep = Math.max(1, rangeSize / partitions);

    List<Range> ranges = new ArrayList<>(partitions);

    long start = min;
    long end = start;

    // keep track of the idealized target to avoid accumulating rounding errors
    long targetStart = min;
    long targetEnd = targetStart;

    // Round to avoid splitting hash domains across ranges if such rounding is not significant.
    // With default bits==16, one would need to create more than 4000 shards before this
    // becomes false by default.
    int mask = 0x0000ffff;
    boolean round = rangeStep >= (1 << bits) * 16;

    while (end < max) {
      targetEnd = targetStart + rangeStep;
      end = targetEnd;

      if (round && ((end & mask) != mask)) {
        // round up or down?
        int increment = 1 << bits;  // 0x00010000
        long roundDown = (end | mask) - increment;
        long roundUp = (end | mask) + increment;
        if (end - roundDown < roundUp - end && roundDown > start) {
          end = roundDown;
        } else {
          end = roundUp;
        }
      }

      // make last range always end exactly on MAX_VALUE
      if (ranges.size() == partitions - 1) {
        end = max;
      }
      ranges.add(new Range((int) start, (int) end));
      start = end + 1L;
      targetStart = targetEnd + 1L;
    }

    return ranges;
  }

  static void printRanges( List<Range> ranges, Integer hash ) {
    int shardCounter = 0;
    for ( Range r : ranges ) {
      shardCounter++;
      System.out.println( "Shard # " + shardCounter );
      System.out.println( "\tRange: "
          + String.format("0x%8s", Integer.toHexString(r.min)).replace(' ', '0')
          + " to "
          + String.format("0x%8s", Integer.toHexString(r.max)).replace(' ', '0')
          );
      if ( null!=hash ) {
        if ( hash >= r.min && hash <= r.max ) {
          System.out.println( "\tcontains "
              + String.format("0x%8s", Integer.toHexString(hash)).replace(' ', '0')
              );
        }
      }
    }
  }
  static int findShardForHash( List<Range> ranges, Integer hash ) {
    int shardCounter = 0;
    for ( Range r : ranges ) {
      shardCounter++;
      if ( hash >= r.min && hash <= r.max ) {
        return shardCounter;
      }
    }
    return -1;
  }

  public static void main(String[] args) {
    if ( args.length < 1 || args.length > 3 ) {
      System.err.println( "Error: syntax: " + HELP_USAGE );
      System.exit(1);
    }
    String docId = args[0];
    if ( docId.length() < 1 ) {
      System.err.println( "Error: empty docId" );
      System.exit(2);      
    }
    String numShardsStr = args.length >= 2 ? args[1] : null;
    String quietStr = args.length >= 3 ? args[2] : null;
    boolean quiet = null!=quietStr && quietStr.equalsIgnoreCase("-q");

    int signedHash = Hash.murmurhash3_x86_32( docId, 0, docId.length(), 0 );
    long unsignedHash = signedHash & 0x00000000ffffffffL;
    if ( ! quiet ) {
      System.out.println( "docId: \"" + docId + '"' );
      System.out.println( "32-bit Hash (signed decimal int): " + signedHash );
      System.out.println( "32-bit Hash (unsigned dec int): " + unsignedHash );
      System.out.println( "32-bit Hash (hex): " + String.format("0x%8s", Integer.toHexString(signedHash)).replace(' ', '0') );
      System.out.println( "32-bit Hash (binary): " + String.format("%32s", Integer.toBinaryString(signedHash)).replace(' ', '0') );
    }
    else {
      System.out.print( docId + " " );
      System.out.print( String.format("0x%8s", Integer.toHexString(signedHash)).replace(' ', '0') );      
    }

    if ( null != numShardsStr ) {
      Integer numShards = null;
      try {
        numShards = Integer.decode( numShardsStr );
      }
      catch( NumberFormatException e ) {
        System.err.println( "Error parsing numberOfShards: " + e );
        System.exit(3);      
      }
      if ( numShards <= 0 ) {
        System.err.println( "Error: numberOfShards must be > 0; got " + numShards );
        System.exit(4);        
      }
      // WRONG!
      // long shardNumber = (unsignedHash % numShards) + 1;
      // System.out.println( "Route to Shard (base-ONE): " + shardNumber );

      List<Range> ranges = partitionRange( numShards );

      if ( ! quiet ) {
        System.out.println( "Number of Shards: " + numShards );

        printRanges( ranges, signedHash );
      }
      else {
        int targetShard = findShardForHash( ranges, signedHash );
        System.out.print( " " + targetShard );
      }
    }
    if ( quiet ) {
      System.out.println();
    }
    
  }

}
