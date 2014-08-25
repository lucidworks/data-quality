package com.lucidworks.dq.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

  public static String NL = System.getProperty("line.separator");

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
