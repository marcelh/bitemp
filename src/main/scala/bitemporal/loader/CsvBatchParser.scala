package bitemporal.loader
import java.io.File
import java.security.MessageDigest
import java.io.FileInputStream
import scala.io.Source

/**
 * Simple implementation of a BatchParser for parsing character-separated-values files.
 */
class CsvBatchParser(val file: File, val separator: Char) extends BatchParser {

    lazy val metaData: Map[String, String] = loadMetaData
	lazy val identifier: String = createSha1Hash(file)
	lazy val name: String = file.getPath()
    
    def processData(f: Map[String, Any] => Unit) {
        val source = Source.fromFile(file)
        try {
            val lines = source.getLines()
            val header = if (lines.hasNext) lines.next().split(separator).toList else List()
            lines.foreach( line => f(header.zip(line.split(separator)).toMap) )
        } finally {
            source.close()
        }
    }
    
    private def loadMetaData: Map[String, String] = Map("batch-id"->identifier, "file-name"->file.getPath)
    
    private def createSha1Hash(file: File) = {
        val md = MessageDigest.getInstance("SHA1")
        val fis = new FileInputStream(file)
        try {
            val dataBytes = new Array[Byte](8 * 1024);
            var nread = fis.read(dataBytes);
            while (nread >= 0) {
                md.update(dataBytes, 0, nread)
                nread = fis.read(dataBytes);
            }
        }
        finally {
            fis.close()
        }
        md.digest().map("%02x" format _).mkString
    }
}