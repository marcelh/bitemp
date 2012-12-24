package bitemporal.loader


trait Parser {

	def id: String
    def name: String
    def metaData: Map[String, Any]
    def hasNext: Boolean
    def next: Map[String, Any]
    def close
}