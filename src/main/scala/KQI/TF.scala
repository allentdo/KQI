package Threshold
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
/**
  * Created by WangHong on 2016/9/16.
  */
case class Http(id:String,f1:Long,f2:Long,f3:Long,f4:Long,f5:Long,f6:Double,f7:Double,f8:Double,f9:Double,f10:Double,f11:Double,f12:Double,f13:Double,f14:Double,f15:Double)
object TF {
  implicit class RString(val raws:String){
    def toL=if (raws.trim.equals("")) 0L else raws.toLong
    def toD=if (raws.trim.equals("")) 0.0 else raws.toDouble
    def /(other:RString)=if(other.toD==0.0) 0.0 else new RString(raws).toD / other.toD
    def /(other:Long)=if(other==0L) 0.0 else new RString(raws).toD/other
    def -(other:RString)=new RString(raws).toL - other.toL
  }
  implicit class RDouble(val d:Double){def zd(i:Int) = if(i==0) 0.0 else d/i}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TF")/*.setMaster(s"local[40]")*/.set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val (times,badr,step,stepScale) = (60,0.02,args(2).toDouble,0.8)//(超限次数，目标比例，步长，步长缩放比，步长浮动范围)
    val flt = args(3).toDouble
    val fPro = (Array(1,2,3,4,5,6,7,8,9).map("f"+_),new Array[Double](9).map(x=>0.005),Array((Array(1,2,3,4,5),5),(Array(6,7,8,9),4)))
    val filterArr = Array(
      (30000,1),(30000,1),(30000,1),(30000,1),(180000,1),
      (100,0),(100,0),(100,0),(100,0), (300000,0),
      (300000,0),(100,0),(100,0),(100,0),(100,0)).map(x=>(x._2.toDouble,x._1.toDouble))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val rd = preproccess(sc.textFile(args(0)),times).map(x=>Http(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16)).toDF().cache()
    val frd=filterFeature(fPro._1,rd,filterArr,sqlContext).cache()
    val num = frd.count()
    val test=findThreshold(badr,flt,fPro._1,fPro._2,step,stepScale,fPro._3,frd,sqlContext,args(1))
      .collect().map(x=>(x._1,{if(x._2<0) -1 else (num-num*x._2).toInt})).map(x=>if(x._2<0) x else (x._1,rd.sort(x._1).select(x._1).rdd.take(x._2).reverse.head.get(0)))
    sc.parallelize(test,1).saveAsTextFile(args(1))
    println("solution:"+test.mkString("[",",","]"))
    sc.stop
  }
  def findThreshold(targ:Double,flg:Double,selF:Array[String],initT:Array[Double],step:Double,stepScale:Double,condition:Array[(Array[Int],Int)],df:DataFrame,sqlc:SQLContext,savedir:String)={
    def overSet(thrdNums:Array[Int],condition:Array[(Array[Int],Int)],df:DataFrame,tempDf:DataFrame,sqlc:SQLContext)={
      val names = df.columns.drop(1)
      val nn = names.map(_.replace("index",""))
      if(names.length!=thrdNums.length) throw new IllegalArgumentException("feature nums not match")
      val scondition=condition.map(x=>(x._1.map(_.toString),x._2))
      sqlc.udf.register("isfilter",(ls:Seq[Int])=>{val nls=nn zip ls zip thrdNums;scondition.map(x=>nls.filter(y=>x._1.contains(y._1._1)).map(y=>if(y._1._2<=y._2) 1 else 0).sum>=x._2).reduce(_&&_)})
      tempDf.filter("isfilter(arr)").count()
    }
    def changeTOnce(step:Double,lastNum:Long,thrds:Array[Double],condition:Array[(Array[Int],Int)],df:DataFrame,tempDf:DataFrame,dfnums:Long,sqlc:SQLContext)={
      var tmp=0L
      val chose=(for (i <- 0 until thrds.length) yield {val tarr=thrds.clone();tarr(i)+=step;tarr}).map(x=>{
        overSet(x.map(x=>(x*dfnums).toInt),condition,df,tempDf,sqlc)}
      ).map(x=>math.abs(lastNum-x))
      var n=0
      val fir=chose(0)
      if(chose.map(_==fir).reduce(_&&_)) n=chose.length else n=1
      print("x._1:  ")
      chose.zipWithIndex.sortBy(_._1).reverse.take(n).foreach(x=>{print(x._1+" , ");thrds(x._2)+=step;tmp+=(if(step>0) 1L else -1L) * x._1})
      println()
      (thrds,overSet(thrds.map(x=>(x*dfnums).toInt),condition,df,tempDf,sqlc))
    }
    val ct = df.count()
    var fg=true
    var (stp,thrds,tmpNums)=(step,initT.clone(),0L)
    val (targNum,flgNum)=((ct * targ).toInt,(ct * flg).toInt)
    val names = df.columns.drop(1)
    val tempDf=df.select(array(names(0),names.drop(1):_*) as "arr").repartition(64).cache
    val time=System.currentTimeMillis()
    while(fg){
      val ret=changeTOnce(stp,tmpNums,thrds,condition,df,tempDf,ct,sqlc)
      thrds=ret._1
      tmpNums=ret._2
      val dif = targNum-tmpNums
      if(math.abs(dif)<flgNum) fg=false else if((dif>0&&stp>0)||(dif<0&&stp<0)) stp=stp else stp= -1.0*stp*stepScale
      println("dif:"+dif+"\n"+"throd:"+thrds.mkString(",")+"\n"+"targ:"+flgNum+"\n"+"step:"+stp+"\n"+"lastNum:"+tmpNums)
    }
    println("time:"+(System.currentTimeMillis-time)*1.0/60000)
    println("thresholds:"+thrds.mkString("[",",","]"))
    sqlc.sparkContext.parallelize((selF zip thrds) union {val di=1 to 15 diff selF.map(_.replace("f","").toInt);di.map("f"+_) zip (1 to di.length).map(x=> -1.0)},1)
  }
  def filterFeature(selF:Array[String],df:DataFrame,fminmax:Array[(Double,Double)],sqlc:SQLContext):DataFrame ={
    sqlc.udf.register("fminmax",(fs:Seq[Double])=>{
      if(fs.length!=fminmax.length) throw new IllegalArgumentException("feature's nums and filter Array nums error")
      fs.zip(fminmax).map(x=>x._1<x._2._2).reduce(_&&_)
    })
    val cols=df.columns.drop(1)
    val nums=selF.map(_.replace("f",""))
    val sdf=df.withColumn("tmp",array(cols(0),cols.drop(1):_*)).filter(s"fminmax(tmp)").select("id",selF:_*)
    selF.map(x=>(x.replace("f","t"),sdf.sort(x).select("id")))
      .map(x=>{var num=0;sqlc.udf.register("index",this.synchronized{()=>{num+=1;num}});x._2.repartition(1).registerTempTable(x._1);val n=x._1.replace("t","");sqlc.sql(s"select index() as index$n,id as id$n from ${x._1}")})
      .reduce((d1,d2)=>d1.join(d2,d1(d1.columns(1))===d2(d2.columns(1)))) .select(s"id${nums(0)}",nums.map(x=>s"index$x"):_*) .withColumnRenamed(s"id${nums(0)}","id");
  }
  def preproccess(rdd:RDD[String],times:Int)={
    rdd.map(_.split(",",-1)).filter(x=>x(0).length>0&&x(0) != null)
      .map(x=>(x(0),x(9)-x(8),x(10)-x(9),x(10)-x(8),x(17).toL,x(21)-x(18), x(14)/x(6),x(13)/x(7),x(12)/x(6),x(11)/x(7),x(4)/(x(21)-x(18)),x(23)/(x(21)-x(18)),x(8),x(9),x(10),x(16),x(17))).groupBy(_._1)
      .map(x=>{val y=x._2.toArray
        val l=if(x._2.size<times) x._2.size else times
        val(k9,k10,k11,k17,k18) = (y.map(_._13),y.map(_._14),y.map(_._15),y.map(_._16),y.map(_._17))
        (x._1,y.map(_._2).sorted.apply(l-1),y.map(_._3).sorted.apply(l-1),y.map(_._4).sorted.apply(l-1),y.map(_._5).sorted.apply(l-1),y.map(_._6).sorted.apply(l-1),
          y.map(_._7).sorted.apply(l-1),y.map(_._8).sorted.apply(l-1),y.map(_._9).sorted.apply(l-1),y.map(_._10).sorted.apply(l-1),
          y.map(_._11).sorted.apply(l-1),y.map(_._12).sorted.apply(l-1),
          k10.count(_.toL != 0)*1.0 zd k9.count(z=>true),k11.count(_.toL != 0)*1.0 zd k10.count(z=>true),k18.count(_.toL != 0)*1.0 zd k18.count(z=>true),k17.count(z=>(z.toL!=0)&&(z.toL<400))*1.0 zd k17.count(_.toL!=0))}
      )
  }
}