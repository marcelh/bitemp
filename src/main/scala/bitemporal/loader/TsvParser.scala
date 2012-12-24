package bitemporal.loader

import scala.collection.immutable.Map
import java.io.File
import java.security.MessageDigest
import java.io.FileInputStream
import scala.io.Source

class TsvParser(file: File) extends Parser {

    val separator = "\t"
    val id: String = createSha1Hash(file)
    val name: String = file.getPath()
    val lineParser = new LineParser(Source.fromFile(file))
    val header = if (lineParser.hasNext) lineParser.next.split(separator).toList else List()
    var recordCount = 0
    
    def metaData(): Map[String, Any] = Map("file" -> file.getPath(), "sha1" -> createSha1Hash(file))

    def hasNext(): Boolean = lineParser.hasNext

    def next(): Map[String, Any] = {
        recordCount += 1
        header.zip(lineParser.next.split(separator)).toMap + ("record_no" -> recordCount)
    }

    def close(): Unit = lineParser.close

    def createSha1Hash(file: File) = {
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