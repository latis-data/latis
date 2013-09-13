package latis.util

object RegEx {
      
    /**
     * Regular expression matching one or more word characters ([a-zA-Z_0-9]).
     */
    val WORD = """\w+"""
    
    /**
     * Regular expression matching a variable name.
     * Limited to alpha-numeric characters and underscore.
     * Nested components may have "." in the name.
     */
    val VARIABLE = WORD //TODO: problem matching in SELECTION.  +"(\\."+WORD+")*"
    
    /**
     * Regular expression matching any reasonable number
     * including sign and scientific notation.
     */
    val NUMBER = "[+|-]?[0-9]*\\.?[0-9]*([e|E][+|-]?[0-9]*)?"
    
    /**
     * Regular expression that should match an ISO 8601 time.
     * No fractional seconds. Not variants without "-".
     */
    val TIME = "[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-2][0-9](:[0-5][0-9](:[0-5][0-9])?)?)?".r
    
    /**
     * Regular expression matching common delimiters: white space and commas.
     */
    val DELIMITER = """[\s,]+""" //white space or comma
    //TODO: will ", , ," be seen as a single delimiter?
    
    /**
     * Regular expression matching the operators: 
     *   >   Greater than
     *   >=  Greater than or equal to
     *   <   Less than
     *   <=  Less than or equal to
     *   ==   Equals
     *   !=  Not equals
     *   =~  Matches pattern
     *   ~   Almost equals, match nearest value
     */
    val SELECTION_OPERATOR = ">=|<=|>|<|=~|==|!=|~"
      
      
    /**
     * Selection clause. Match variable name, operator, value.
     */
    val SELECTION = ("(" + VARIABLE + ")(" + SELECTION_OPERATOR + ")(.+)").r
    
    /**
     * Projection clause. Match comma separates list of variable names.
     */
    val PROJECTION = ("(" + VARIABLE + "(," + VARIABLE + ")*)").r
    
    /**
     * Operation expression. Match operation name and argument list.
     */
    val OPERATION = ("(" + WORD + ")\\((.*)\\)").r
}