package bitemporal.loader

import scala.io.Source

import org.joda.time.Interval

import bitemporal.BitemporalRepository

/**
 * Simple implementation of a Batch for parsing character-separated-values files.
 */
class CsvBatch(val source: Source, val batchId: String, val name: String, val separator: Char) extends Batch {

	def metaData: Map[String, String] = {
		Map("batch-id" -> batchId, 
	        "batch-name" -> name,
	        "batch-separator" -> separator.toString)
    }
	
    def recordIterator: Iterator[Record] = {
        val lines = source.getLines()
        val header = if (lines.hasNext) lines.next().split(separator).toList else List()
            
        new Iterator[Record] {
            val iter = lines zipWithIndex
            
            def hasNext: Boolean = iter.hasNext
            
            def next(): Record = { 
                val (line, no) = iter.next()
    			val start = BitemporalRepository.startOfTime
    			val end = BitemporalRepository.endOfTime
                new Record {
                    val recordId = name + "[" + no + "]"
                    val validInterval = new Interval(start, end)
                    val values = header.zip(line.split(separator)).toMap
                }
        	}
        }
    }
}
