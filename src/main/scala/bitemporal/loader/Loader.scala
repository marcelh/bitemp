package bitemporal.loader
import java.io.File

trait Loader {
    
    def load(parser: Parser)
}