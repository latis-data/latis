package latis.util

/**
 * A collection of handy regular expressions used to parse constraint expressions.
 */
object RegEx {
  //TODO: consider Parser Combinators
      
    /**
     * Regular expression matching one or more word characters ([a-zA-Z_0-9]).
     */
    lazy val WORD = """\w+"""
    
    /**
     * Regular expression matching a variable name.
     * Limited to alpha-numeric characters and underscore.
     * Nested components may have "." in the name.
     */
    lazy val VARIABLE = s"$WORD(?:\\.$WORD)*"
    
    /**
     * Regular expression matching any reasonable number
     * including sign and scientific notation.
     */
    lazy val NUMBER = """[+|-]?(?:\d*\.\d+|\d+\.\d*|\d+)(?:[e|E][+|-]?\d+)?"""
      
    /**
     * Regular expression matching any reasonable value (e.g. on the right hand side on an expression.
     * This is designed to keep out escape characters and other nasties.
     */
    lazy val VALUE = """[\w\.\+\-eE\:\*\?\{\}\<\>#\s/'=]+"""
    //Note, needed to match regex for "=~" op: "*?"
    //  SPARQL: "{}<>#?/" and space
    //TODO: find alternative to being so liberal:
    //  just don't support such queries, not a good idea anyway?
    //  require encapsulation within 'quotes'?
    //TODO: work out the kinks in: lazy val VALUE = s"(?:$TIME)|(?:$NUMBER)|(?:$VARIABLE)" //TODO: OPERATION? but recursive
    
    /**
     * Regular expression that should match an ISO 8601 time or the form: yyyy-MM-ddTHH:mm:ss
     * No fractional seconds. Not variants without "-".
     */
    lazy val TIME = "[0-9]{4}-[0-9]{2}-[0-9]{2}(?:'?T'?[0-2][0-9](?::[0-5][0-9](?::[0-5][0-9])?)?)?"
    
    /**
     * Regular expression matching common delimiters: white space and commas.
     */
    lazy val DELIMITER = """\s*,\s*|\s+"""
    
    /**
     * Regular expression matching the operators: 
     *   >   Greater than
     *   >=  Greater than or equal to
     *   <   Less than
     *   <=  Less than or equal to
     *   =    Equals
     *   ==   Equals
     *   !=  Not equals
     *   =~  Matches regex pattern
     *   !=~ Does not match regex pattern
     *   ~   Almost equals, match nearest value
     */
    lazy val SELECTION_OPERATOR = ">=|<=|>|<|=~|==|!=|=|~|!=~"
      
      
    /**
     * Selection clause. Match variable name, operator, value.
     * Allow white space around the operator.
     */
    lazy val SELECTION = s"($VARIABLE)\\s*($SELECTION_OPERATOR)\\s*($VALUE|'$VALUE')"
    
    /**
     * Projection clause. Match comma and/or space delimited list of variable names.
     * Don't try to match groups since we have a variable number of groups and this
     * would only capture the first and last elements.
     */
    lazy val PROJECTION = s"($VARIABLE(?:(?:$DELIMITER)$VARIABLE)*)"
    
    /**
     * List of arguments, such as for a function call.
     * Don't try to match groups.
     */
    lazy val ARGUMENT_LIST = s"$VALUE(?:,\\s*$VALUE)*"
    
    /**
     * Operation expression (function call with parens). 
     * Match operation name and argument list.
     */
    lazy val OPERATION = s"($WORD)\\(\\s*($ARGUMENT_LIST)*\\s*\\)"
    
}
