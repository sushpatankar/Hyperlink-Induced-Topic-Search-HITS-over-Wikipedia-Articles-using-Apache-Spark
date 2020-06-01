import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object P1 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("P1").setMaster("Yarn")
    val cont= new SparkContext()

    val mlink= cont.textFile(args(0)).map(link=> (link.split(':')(0).toLong, link.split(':')(1).trim.split(' '))).flatMapValues(s=> s).map{case(node ,link ) => (node, link.toLong)}
    val mtitle= cont.textFile(args(1)).zipWithIndex().map{ case( k, index) => ( index + 1 , k) }

    val ftitle= mtitle.filter{case(id, title )=> title.toLowerCase().contains(args(2))}
    val LinkedOut = mlink.join(ftitle).map{case (k,(v1,v2))=>(k,v1)}


    val LinkedS= mlink.map{case (k,olinks)=>(olinks,k)}
    val Linked= LinkedS.join(LinkedOut).map{case (k,(v1,v2))=>(v1,k)}.distinct()
    val aggregateLink= LinkedOut.union(Linked)

    var Hub = aggregateLink.map{ case(k, v ) => (k, 1.0) }
    var Auth = aggregateLink.map{ case(k, v ) => (k, 1.0) }

    for(i<-1 to 50){
      
      val InitAuth = aggregateLink.join(Hub).distinct().map{case(k,(v1, v2)) => (k, v2 ) }.reduceByKey(_+_)
      val InitHub =aggregateLink.map{case(k,v) => (v, k)}.join(Auth).distinct().map{case(key,(v1, v2) ) => (v1, v2)}

      Auth = InitAuth.map{case(key, value) => (key, ( value /  InitAuth.values.sum()) ) }
      Hub = InitHub.map{case(key, value) => (key,( value / InitHub.values.sum()) )}

    }
    var auth_sorted = Auth.join(ftitle).map{case(k, (v1, v2)) => (v1, (k, v2))}.sortByKey(false).map{case(v1, (k, v2)) => ( k, (v2, v1) ) }.coalesce(1).saveAsTextFile(args(3))

    var hub_sorted = Hub.join(ftitle).map{case(k, (v1, v2)) => (v1, (k, v2))}.sortByKey(false).map{case(v1, (k, v2)) => ( k, (v2, v1) ) }.coalesce(1).saveAsTextFile(args(4))
  }

}
