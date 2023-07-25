package david.sc_dbscan.objects


object TripleBuilder {

  def createTriple(line: String): Triple = {

    var tripleTab = line.split(" ")

    return new Triple(tripleTab(0), tripleTab(1), tripleTab(2))
  }

}


class Triple(subject: String, property: String, value: String) extends Serializable {

  def getSubject(): String = this.subject

  def getProperty(): String = this.property

  def getValue(): String = this.value

  def canEqual(a: Any): Boolean = a.isInstanceOf[Triple]

  override def equals(that: Any): Boolean = {
    that match {
      case that: Triple => that.canEqual(this) && this.getSubject() == that.getSubject() && this.getProperty() == that.getProperty() && this.getValue() == that.getValue()
      case _ => false
    }
  }

  override def toString(): String = this.subject+" "+this.property+" "+this.value

  override def hashCode: Int = this.toString().hashCode()


}

