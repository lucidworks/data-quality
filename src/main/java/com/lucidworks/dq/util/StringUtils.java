package com.lucidworks.dq.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

  public static String NL = System.getProperty("line.separator");

  public static String parseAndCatchGroupAsStringOrNull( String patternStr, String sourceText, int groupNumber ) {
    // Java caches pattern compilations
    Pattern pattern = Pattern.compile( patternStr );
    Matcher matcher = pattern.matcher( sourceText );   
    if ( matcher.find() ) {
      return matcher.group( groupNumber );
    }
    else {
      return null;
    }
  }

  public static Long parseAndCatchGroupAsLongOrNull( String patternStr, String sourceText, int groupNumber ) {
    String matchStr = parseAndCatchGroupAsStringOrNull( patternStr, sourceText, groupNumber );
    if ( null!=matchStr ) {
      // Java caches valueOf object results
      return Long.valueOf( matchStr );
    }
    else {
      return null;
    }
  }

  /*
   * Convert:
   *  color=red&fruit=apple&fruit=banana&desert=&lone+string
   * into:
   *  { color: [red], fruit: [apple, banana], desert: [""], content: [pizza] }
   * TODO: feels like reinventing the wheel, though want very specific rules applied...
   */
  public static Map<String,Collection<String>> parseCgiParameters( String rawText ) {

    // picky options we might expose later on
    boolean maintainInsertionOrder = false;
    boolean isCaseSensitiveKeys = true;
    boolean trimKeys = true;
    String defaultParamName = "content";
    String encoding = "UTF-8";
    // Value normalization might vary by parameter name
    // TODO: separate method to look for CSV and space delimited values
    // TODO: separate method to perhaps provide default values

    // Map<String,Collection<String>> outMap = maintainInsertionOrder ? new LinkedHashMap<>() : new TreeMap<>();
    Map<String,Collection<String>> outMap = null;
    if ( maintainInsertionOrder ) {
      outMap = new LinkedHashMap<>();
    }
    else {
      outMap = new TreeMap<>();
    }

    // Break on & and ? (usually just &)
    String [] args = rawText.split( "[?&]" );
    for ( int i=0; i<args.length; i++ ) {

      String arg = args[i];
      // Skip empty entries
      if ( arg.isEmpty() ) {
        continue;
      }

      // Break on FIRST equals sign with arg
      int equalsAt = arg.indexOf( '=' );
      String key = "";
      String value = "";
      if ( equalsAt >= 0 ) {
        if ( equalsAt > 0 ) {
          key = arg.substring( 0, equalsAt );
        }
        if ( equalsAt < arg.length() ) {
          value = arg.substring( equalsAt + 1 );
        }
      }
      else {
        key = arg;
      }

      // Normalize key and value
      try {
        key = URLDecoder.decode( key, encoding );
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      if ( trimKeys ) {
        key = key.trim();
      }
      if ( key.isEmpty() ) {
        key = defaultParamName;
      }
      if ( ! isCaseSensitiveKeys ) {
        key = key.toLowerCase();
      }
      // normalization of values can be handled via additioanl methods
      try {
        value = URLDecoder.decode( value, encoding );
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      // Tabulate
      Collection<String> values = null;
      if ( outMap.containsKey(key) ) {
        values = outMap.get(key);
      }
      else {
        values = new ArrayList<>();
        outMap.put( key, values );
      }
      values.add( value );
      
    }
  
    return outMap;
  }

  // AKA multiplyString
  // TODO: Or use org.apache.commons.lang.StringUtils.repeat(...) ???
  public static String repeatString( String s, int n ) {
    if ( n <= 0 ) { return ""; }
    // http://stackoverflow.com/questions/1235179/simple-way-to-repeat-a-string-in-java
    return new String(new char[n]).replace("\0", s);
    // return String.format(String.format("%%0%dd", n), 0).replace("0",s);
  }

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

  public static String escapeSpaces( String inStr ) {
    if ( null==inStr ) {
      return null;
    }
    return inStr.replaceAll( "[ ]", "\\\\ " );
  }
  public static String escapeColons( String inStr ) {
    if ( null==inStr ) {
      return null;
    }
    return inStr.replaceAll( "[:]", "\\\\:" );
  }

  /**
   * Based on code from:
   * http://stackoverflow.com/questions/1247772 and
   * http://stackoverflow.com/a/17369948/295802
   * 
   * Converts a standard POSIX Shell globbing pattern into a regular expression
   * pattern. The result can be used with the standard {@link java.util.regex} API to
   * recognize strings which match the glob pattern.
   * <p/>
   * See also, the POSIX Shell language:
   * http://pubs.opengroup.org/onlinepubs/009695399/utilities/xcu_chap02.html#tag_02_13_01
   * 
   * @param pattern A glob pattern.
   * @return A regex pattern to recognize the given glob pattern.
   */
  public static final String convertGlobToRegex(String pattern) {
    StringBuilder sb = new StringBuilder(pattern.length() * 2);
    int inGroup = 0;
    int inClass = 0;
    int firstIndexInClass = -1;
    char[] arr = pattern.toCharArray();
    for (int i = 0; i < arr.length; i++) {
      char ch = arr[i];
      switch (ch) {
      case '\\':
        if (++i >= arr.length) {
          sb.append('\\');
        } else {
          char next = arr[i];
          switch (next) {
          case ',':
            // escape not needed
            break;
          case 'Q':
          case 'E':
            // extra escape needed
            sb.append('\\');
          default:
            sb.append('\\');
          }
          sb.append(next);
        }
        break;
      case '*':
        if (inClass == 0)
          sb.append(".*");
        else
          sb.append('*');
        break;
      case '?':
        if (inClass == 0)
          sb.append('.');
        else
          sb.append('?');
        break;
      case '[':
        inClass++;
        firstIndexInClass = i + 1;
        sb.append('[');
        break;
      case ']':
        inClass--;
        sb.append(']');
        break;
      case '.':
      case '(':
      case ')':
      case '+':
      case '|':
      case '^':
      case '$':
      case '@':
      case '%':
        if (inClass == 0 || (firstIndexInClass == i && ch == '^'))
          sb.append('\\');
        sb.append(ch);
        break;
      case '!':
        if (firstIndexInClass == i)
          sb.append('^');
        else
          sb.append('!');
        break;
      case '{':
        inGroup++;
        sb.append('(');
        break;
      case '}':
        inGroup--;
        sb.append(')');
        break;
      case ',':
        if (inGroup > 0)
          sb.append('|');
        else
          sb.append(',');
        break;
      default:
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  // TODO: could also do list of matches with m.reset(myNewString), might be slightly faster
  public static boolean checkPatternsInList( Collection<Pattern> patterns, String targetString ) {
    for ( Pattern p : patterns ) {
      Matcher m = p.matcher( targetString );
      if ( m.matches() ) {
        return true;
      }
    }
    return false;
  }


}
