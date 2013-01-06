package bitemporal.loader
import java.io.File

trait BatchParser {

    def metaData: Map[String, String]
    def identifier: String
    def name: String
    def processData(f: Map[String, Any] => Unit)
}