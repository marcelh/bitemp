package bitemporal.util

import java.io.File
import resource._
import java.security.MessageDigest
import java.io.FileInputStream

object Sha1Hash {

    def forFile(file: File): String = {
        val md = MessageDigest.getInstance("SHA1")
        for (fis <- managed(new FileInputStream(file))) {
            val dataBytes = new Array[Byte](8 * 1024);
            var nread = fis.read(dataBytes);
            while (nread >= 0) {
                md.update(dataBytes, 0, nread)
                nread = fis.read(dataBytes);
            }
        }
        md.digest().map("%02x" format _).mkString
    }
}