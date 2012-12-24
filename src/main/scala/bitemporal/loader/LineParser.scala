package bitemporal.loader
import scala.io.Source

/**
 * Simple wrapper around Source to easily parse lines.
 */
class LineParser(val source: Source) {
    
    val lines = source.getLines
    var lastLine: Option[String] = None
    
    /**
     * @return the next line (if any) but do not consume
     */
    def peek: Option[String] = {
        if (lastLine.isEmpty && lines.hasNext) 
            lastLine = Some(lines.next())
        lastLine
    }
    
    /**
     * @return the next line
     * @throws IllegalStateException if there is no next line
     */
    def next: String = {
        if (!hasNext) throw new IllegalStateException("next called but no more input")
        val s = peek.get
        lastLine = None
        s
    }
    
    /**
     * @return if there is another line in the input
     */
    def hasNext: Boolean = peek.isDefined
    
    /**
     * Expect that the next line matches the given expected string and consume the line.
     * @throws IllegalStateException if there is no next line or it is not what was expected 
     */
    def expect(expected: String) {
        if (!hasNext) throw new IllegalStateException("next called but no more input")
        if (expected != peek.get)
            throw new IllegalStateException("expected '" + expected + "' but got '" + peek.get + "'")
        next
    }
    
    /**
     * Close the underlaying Source
     */
    def close { source.close }
}