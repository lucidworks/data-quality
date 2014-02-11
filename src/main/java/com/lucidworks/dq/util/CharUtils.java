package com.lucidworks.dq.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Character.UnicodeBlock;
import java.lang.Character.UnicodeScript;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class CharUtils {
  // Special handling, Unicode mangled character marker
  static final String QUESTION_MARK_STR       = "?";
  static final int    QUESTION_MARK_CODEPOINT = QUESTION_MARK_STR.codePointAt(0);
  static final String QUESTION_MARK_NAME      = "QUESTION_MARK";

  static final Map<Integer,String> TYPES = new HashMap<Integer,String>() {{
    // put( 1, "R / DIRECTIONALITY_RIGHT_TO_LEFT" );
    // put( 2, "AL / DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC" );
    // put( 11, "S / DIRECTIONALITY_SEGMENT_SEPARATOR" );
    // put( 12, "WS / DIRECTIONALITY_WHITESPACE" );
    put( 1, "Lu_UPPERCASE_LETTER" );
    put( 2, "Ll_LOWERCASE_LETTER" );
    put( 3, "Lt_TITLECASE_LETTER" );
    put( 4, "Lm_MODIFIER_LETTER" );
    put( 5, "Lo_OTHER_LETTER" );
    put( 6, "Mn_NON_SPACING_MARK" );
    put( 7, "Me_ENCLOSING_MARK" );
    put( 8 , "Mc_COMBINING_SPACING_MARK" );
    put( 9, "Nd_DECIMAL_DIGIT_NUMBER" );
    put( 11, "No_OTHER_NUMBER" );
    put( 12, "Zs_SPACE_SEPARATOR" );
    put( 13, "Zl_LINE_SEPARATOR" );
    put( 14, "Zp_PARAGRAPH_SEPARATOR" );
    put( 15, "Cc_CONTROL" );
    put( 16, "Cf_FORMAT" ); // or SIZE or DIRECTIONALITY_RIGHT_TO_LEFT_EMBEDDING
    // 17?
    put( 18, "Co_PRIVATE_USE" );
    put( 19, "Cs_SURROGATE" );
    put( 20, "Pd_DASH_PUNCTUATION" );
    put( 21, "Ps_START_PUNCTUATION" );
    put( 22, "Pe_END_PUNCTUATION" );
    put( 23, "Pc_CONNECTOR_PUNCTUATION" );
    put( 24, "Po_OTHER_PUNCTUATION" );
    put( 25, "Sm_MATH_SYMBOL" );
    put( 26, "Sc_CURRENCY_SYMBOL" );
    put( 27, "Sk_MODIFIER_SYMBOL" );
    put( 28, "So_OTHER_SYMBOL" );
    put( 29, "Pi_INITIAL_QUOTE_PUNCTUATION" );
    put( 30, "Pf_FINAL_QUOTE_PUNCTUATION" );
  }};

  static final Map<String,String> ALIASES_SHORT_TO_LONG = new HashMap<String,String>() {{
    // Custom
	put( "Qm", QUESTION_MARK_NAME );

	// Script
	put( "Com", "COMMON" );
	put( "Lat", "LATIN" );

	// Block
	put( "Basic", "BASIC_LATIN" );
	put( "L1Sup", "LATIN_1_SUPPLEMENT" );
	put( "GenPunct", "GENERAL_PUNCTUATION" );
	put( "LetterSym", "LETTERLIKE_SYMBOLS" );
	
	// Types
	put( "UPPER", "Lu_UPPERCASE_LETTER" );
    put( "lower", "Ll_LOWERCASE_LETTER" );
    put( "Title", "Lt_TITLECASE_LETTER" );
    put( "ModL", "Lm_MODIFIER_LETTER" );
    put( "OtherL", "Lo_OTHER_LETTER" );
    put( "NonSpc", "Mn_NON_SPACING_MARK" );
    put( "Encl", "Me_ENCLOSING_MARK" );
    put( "Combining" , "Mc_COMBINING_SPACING_MARK" );
    put( "Digit", "Nd_DECIMAL_DIGIT_NUMBER" );
    put( "OtherNum", "No_OTHER_NUMBER" );
    put( "Space", "Zs_SPACE_SEPARATOR" );
    put( "Line", "Zl_LINE_SEPARATOR" );
    put( "Para", "Zp_PARAGRAPH_SEPARATOR" );
    put( "Ctrl", "Cc_CONTROL" );
    put( "Fmt", "Cf_FORMAT" ); // or SIZE or DIRECTIONALITY_RIGHT_TO_LEFT_EMBEDDING
    // 17?
    put( "Priv", "Co_PRIVATE_USE" );
    put( "Sur", "Cs_SURROGATE" );
    put( "Dash", "Pd_DASH_PUNCTUATION" );
    put( "Start", "Ps_START_PUNCTUATION" );
    put( "End", "Pe_END_PUNCTUATION" );
    put( "Conn", "Pc_CONNECTOR_PUNCTUATION" );
    put( "OtherP", "Po_OTHER_PUNCTUATION" );
    put( "Math", "Sm_MATH_SYMBOL" );
    put( "Currency", "Sc_CURRENCY_SYMBOL" );
    put( "ModSym", "Sk_MODIFIER_SYMBOL" );
    put( "OtherSym", "So_OTHER_SYMBOL" );
    put( "StartQ", "Pi_INITIAL_QUOTE_PUNCTUATION" );
    put( "EndQ", "Pf_FINAL_QUOTE_PUNCTUATION" );
  }};
 
  static final Map<String,String> ALIASES_LONG_TO_SHORT = new HashMap<String,String>();
  static {
    for ( Entry<String,String> entry : ALIASES_SHORT_TO_LONG.entrySet() ) {
      String shortName = entry.getKey();
      String longName = entry.getValue();
      ALIASES_LONG_TO_SHORT.put( longName, shortName );
    }
  }

  // Compound Aliases
  // Note: reversed order of initialization here
  static final Map<String,String> COMPOUND_ALIASES_LONG_TO_SHORT = new HashMap<String,String>() {{
	put( "Com-Basic-Space", "space" );
	put( "Lat-Basic-UPPER", "UPPER" );
	put( "Lat-Basic-lower", "lower" );
	put( "Com-Basic-Conn", "Connector" );
	put( "Com-Basic-Currency", "Currency" );
	put( "Com-Basic-Digit", "Digit" );
	put( "Com-Basic-OtherP", "OtherPunct" );
	put( "Com-L1Sup-OtherSym", "OtherSym" );
	put( "Com-Basic-Start", "Start" );
	put( "Com-Basic-End", "Stop" );
	put( "Com-Basic-Math", "Math" );
	put( "Com-Basic-Dash", "Dash1" );
	put( "Com-GenPunct-Dash", "Dash2" );
	put( "Com-LetterSym-OtherSym", "LetterSymbol" );
	put( "Com-Basic-Qm", "QuestionMark" );  // add suffix 1 when needed
  }};
  static final Map<String,String> COMPOUND_ALIASES_SHORT_TO_LONG = new HashMap<String,String>();
  static {
    for ( Entry<String,String> entry : COMPOUND_ALIASES_LONG_TO_SHORT.entrySet() ) {
      String longName = entry.getKey();
      String shortName = entry.getValue();
      COMPOUND_ALIASES_SHORT_TO_LONG.put( shortName, longName );
    }
  }

  static String generateReport() {
	return generateReportForRange( 0, 255 );
  }
  static String generateReportForRange( int min, int max ) {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    for ( int i=min; i<=max; i++ ) {
      addCharInfoToReport( out, i );
    }

    String outStr = sw.toString();
    return outStr;
  }
  static String generateReportForPoints( int ... codePoints ) {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    for ( int i : codePoints ) {
      addCharInfoToReport( out, i );
    }

    String outStr = sw.toString();
    return outStr;
  }
  static void addCharInfoToReport( PrintWriter out, int codePoint ) {
	out.print( "" + codePoint );
	out.print( ", " );
	out.print( String.format("%X", codePoint) );
	out.print( ": " );
	if ( codePoint >= 32 ) {
	  Character c = new Character( (char)codePoint );
	  if ( ! Character.isSupplementaryCodePoint( codePoint ) ) {
	    out.print( " c='"+c+"'" );
	  }
	  // Extended / Supplmental Unicode
	  else {
		// also StringBuffer appendCodePoint(int cp)
		char[] chars = Character.toChars( codePoint );
		out.print( " c='" );
		for ( char cS : chars ) {
		  out.print( cS );
		}
		out.print( "'" );
	  }
	}
    boolean isDef = Character.isDefined( codePoint );
	out.print( " isDef="+isDef );
    boolean isValid = Character.isValidCodePoint( codePoint );
	out.print( " isValid="+isValid );
	boolean isCtrl = Character.isISOControl( codePoint );
	out.print( " isCtrl="+isCtrl );
    boolean isBmp = Character.isBmpCodePoint( codePoint );
	out.print( " isBmp="+isBmp );
    boolean isSupp = Character.isSupplementaryCodePoint( codePoint );
	out.print( " isSupp="+isSupp );
    boolean isAlpha = Character.isAlphabetic( codePoint );
	out.print( " isAlpha="+isAlpha );
    boolean isLetter = Character.isLetter( codePoint );
	out.print( " isLetter="+isLetter );
    boolean isDigit = Character.isDigit( codePoint );
	out.print( " isDigit="+isDigit );
    int type = Character.getType( codePoint );
	String typeStr = "" + type;
	if ( TYPES.containsKey(type) ) {
	  typeStr += " " + TYPES.get(type);
	}
	else {
	  typeStr += " (no-TYPES-entry)";
	}
	out.print( " type="+typeStr );
    String block = null;
    String script = null;
    try {
    	block = UnicodeBlock.of( codePoint ).toString();
    	script = UnicodeScript.of( codePoint ).toString();
    }
    catch( Exception e ) { }
	out.print( " script="+script );
	out.print( " block="+block );
    String name = Character.getName( codePoint );
	out.print( " name="+name );
	out.println();
  }

  public static String getScriptName_LongForm( int codePoint ) {
    String script = "Unknown_Unicode_Script";
    try {
    	script = UnicodeScript.of( codePoint ).toString();
    }
    catch( Exception e ) { }
	return script;
  }
  public static String getScriptName_ShortForm( int codePoint ) {
	String longName = getScriptName_LongForm( codePoint );
	if ( ALIASES_LONG_TO_SHORT.containsKey(longName) ) {
	  return ALIASES_LONG_TO_SHORT.get(longName);
	}
	else {
	  return longName;
	}
  }
  public static String getBlockName_LongForm( int codePoint ) {
    String block = "Unknown_Unicode_Block";
    try {
    	block = UnicodeBlock.of( codePoint ).toString();
    }
    catch( Exception e ) { }
    return block;	
  }
  public static String getBlockName_ShortForm( int codePoint ) {
    String longName = getBlockName_LongForm( codePoint );
	if ( ALIASES_LONG_TO_SHORT.containsKey(longName) ) {
	  return ALIASES_LONG_TO_SHORT.get(longName);
	}
	else {
	  return longName;
	}
  }
  public static String getTypeName_LongForm( int codePoint ) {
	int type = Character.getType( codePoint );
	String typeStr = "";
	if ( codePoint == QUESTION_MARK_CODEPOINT ) {
	  typeStr = QUESTION_MARK_NAME;
	}
	else if ( TYPES.containsKey(type) ) {
	  typeStr = TYPES.get(type);
	}
	else {
	  typeStr = "" + type + "_No_TYPES_Entry";
	}
	return typeStr;
  }
  public static String getTypeName_ShortForm( int codePoint ) {
    String longName = getTypeName_LongForm( codePoint );
	if ( ALIASES_LONG_TO_SHORT.containsKey(longName) ) {
	  return ALIASES_LONG_TO_SHORT.get(longName);
	}
	else {
	  return longName;
	}
  }
  // returns "script-block-type"
  public static String getCompoundClassifier_LongForm( int codePoint ) {
    return    getScriptName_LongForm(codePoint)
      + "-" + getBlockName_LongForm(codePoint)
      + "-" + getTypeName_LongForm(codePoint)
      ;
  }
  public static String getCompoundClassifier_ShortForm( int codePoint ) {
    String candidate = getScriptName_ShortForm(codePoint)
               + "-" + getBlockName_ShortForm(codePoint)
               + "-" + getTypeName_ShortForm(codePoint)
               ;
    if ( COMPOUND_ALIASES_LONG_TO_SHORT.containsKey(candidate) ) {
      return COMPOUND_ALIASES_LONG_TO_SHORT.get( candidate );
    }
    else {
      return candidate;
    }
  }
  
  public static Map<String,Long> classifyString_LongForm( String inStr ) {
	return classifyString_LongForm( inStr, null );
  }
  public static Map<String,Long> classifyString_LongForm( String inStr, Map<String,Long> stats ) {
	// Automatically sorts by key-order
	if ( null==stats ) {
	  // In order by key, easier for overall tabulation
	  stats = new TreeMap<>();
	}
	if ( null==inStr || inStr.isEmpty() ) {
	  return stats;
	}
	// Special looping to allow for Supplementary Unicode Characters (> 65k)
	int length = inStr.length();
	for (int offset = 0; offset < length; ) {
	  int codePoint = inStr.codePointAt( offset );
	  String charKey = getCompoundClassifier_LongForm( codePoint );
	  // Tabulate
	  long count = 0L;
	  if ( stats.containsKey(charKey) ) {
		count = stats.get( charKey );
	  }
	  count++;
	  stats.put( charKey, count );
	  // Advance
	  offset += Character.charCount( codePoint );
    }
	return stats;
  }
  public static Map<String,Long> classifyString_ShortForm( String inStr ) {
	return classifyString_ShortForm( inStr, null );
  }
  // TODO: code very similar to LongForm, combine
  public static Map<String,Long> classifyString_ShortForm( String inStr, Map<String,Long> stats ) {
	// Automatically sorts by key-order
	if ( null==stats ) {
	  // In order by key, easier for overall tabulation
	  stats = new TreeMap<>();
	}
	if ( null==inStr || inStr.isEmpty() ) {
	  return stats;
	}
	// Special looping to allow for Supplementary Unicode Characters (> 65k)
	int length = inStr.length();
	for (int offset = 0; offset < length; ) {
	  int codePoint = inStr.codePointAt( offset );
	  String charKey = getCompoundClassifier_ShortForm( codePoint );
	  // Tabulate
	  long count = 0L;
	  if ( stats.containsKey(charKey) ) {
		count = stats.get( charKey );
	  }
	  count++;
	  stats.put( charKey, count );
	  // Advance
	  offset += Character.charCount( codePoint );
    }
	return stats;
  }
  
  public static void main( String [] argv ) {
	// U+306E, dec:12398
	System.out.println( "Japanese \"no\": '\u306e'" );
	// U+4e00 19968, U+4e8c 20108, U+4e09 19977
	System.out.println( "Chinese 1 2 3: '\u4e00\u4e8c\u4e09'" );
	// U+1D11E, dec:119070
	System.out.println( "Extended: Musical G-clef: '\uD834\uDD1E'" );
	// U+1F37A, dec:127866
	System.out.println( "Extended: Beer Mug: '\uD83C\uDF7A'" );

	// String report = generateReportForRange( 0, 255 );
	String report = generateReportForPoints( 12398, 19968, 20108, 19977, 119070, 127866 );
	System.out.print( report );
  }
}