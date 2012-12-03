package bitemporal

sealed trait Attribute {
    type ValueType
    def id: String
    def createValue(value: ValueType): AttributeValue
}

case class StringAttribute(id: String) extends Attribute {
    type ValueType = String
    def createValue(value: String) = StringAttributeValue(this, value)
}

case class IntAttribute(id: String) extends Attribute {
    type ValueType = Int
    def createValue(value: Int) = IntAttributeValue(this, value)
}

sealed trait AttributeValue {
    def attribute: Attribute
}
case class StringAttributeValue(attribute: Attribute, value: String) extends AttributeValue
case class IntAttributeValue(attribute: Attribute, value: Int) extends AttributeValue