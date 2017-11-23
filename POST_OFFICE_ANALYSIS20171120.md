# 修正後程式碼
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date



case class ResultSchema1(PKGNO:String, STATUSA2:String, STATUSB1:String, STATUSB2:String, PROC_DATEA:String, PROC_TIMEA:String, PROC_DTA:String, PROC_BRHA:String, PROC_DATEB:String, PROC_TIMEB:String, PROC_DTB:String, PROC_BRHB:String, PKGVALUE:String, PKGCOLL:String, PKGWEIGHT:String, PKGPOST:String, CONTRACTNO1:String, CONTRACTNO2:String, DESTZIPCODE:String, DESTZIPCODE_LEN:Int, PROC_YMA:String, PROC_YMB:String)
case class ResultSchema2(PKGNO:String, PROC_H:String, REASON:String, REC_BRH:String, REC_BRH_TEL:String, HOURS:Float)

object Main {
  def main(args:Array[String]):Unit = {
    def getTimeStamp(s: String): Timestamp = {
      val format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
      val d = format.parse(s)
      val t = new Timestamp(d.getTime())
      return t
    }

    def getSortValue(v: Iterable[String]): String = {
      val l = v.toList.sorted; return l(0)
    }

    def getRecord(r: (String, List[String])): Option[List[String]] = {
      val res = r._2.find(s => s.slice(0, 0 + 13) > r._1.slice(0, 0 + 13))
      if (res.isDefined) {
        Some(List(r._1, res.get))
      } else None
    }

    def getHours(b: String, e: String): String = {
      val h = "%.2f".format((e.toDouble - b.toDouble) / 1000 / 3600); return h
    }


    val pkgCodeLst = List("70", "71", "72", "73", "74", "75", "76", "78")
    val conf = new SparkConf().setAppName("post office poc").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val a = sc.textFile("hdfs://quickstart.cloudera:8020/tmp/sam03.txt").filter(r => pkgCodeLst.contains(r.slice(14, 14 + 2))).cache()
    val akv = a.filter(r => r.matches("^A.*")).distinct().map(r => (r.slice(2, 2 + 20), "%s%s".format(getTimeStamp(r.slice(22, 22 + 18)).getTime().toString, r))).groupByKey().mapValues(getSortValue(_))
    val hikv = a.filter(r => (r.matches("^H.*") || r.matches("^I.*"))).distinct().map(r => (r.slice(2, 2 + 20), "%s%s".format(getTimeStamp(r.slice(22, 22 + 18)).getTime().toString, r))).groupByKey().mapValues(_.toList.sorted)

    a.unpersist()
    val reskv = akv.join(hikv).mapValues(getRecord(_).getOrElse(List())).values.filter(!_.isEmpty).cache()
    val restb1 = reskv.map(c => Array(c(0).slice(15, 15 + 20), c(0).slice(14, 14 + 1), c(1).slice(13, 13 + 1), c(1).slice(14, 14 + 1), c(0).slice(35, 35 + 10) + " 00:00:00", c(0).slice(45, 45 + 8), c(0).slice(35, 35 + 10) + " " + c(0).slice(45, 45 + 8), c(0).slice(53, 53 + 6), c(1).slice(35, 35 + 10) + " 00:00:00", c(1).slice(45, 45 + 8), c(1).slice(35, 35 + 10) + " " + c(1).slice(45, 45 + 8), c(1).slice(53, 53 + 6), c(0).slice(59, 59 + 6), c(0).slice(65, 65 + 6), c(0).slice(71, 71 + 6), c(0).slice(77, 77 + 6), c(0).slice(83, 83 + 6), c(0).slice(89, 89 + 7), c(0).slice(96, 96 + 5), c(0).slice(96, 96 + 5).trim(), c(0).slice(35, 35 + 4) + c(0).slice(40, 40 + 2), c(1).slice(35, 35 + 4) + c(1).slice(40, 40 + 2))).map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21) => ResultSchema1(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19.length, s20, s21) }.toDF()
    val restb2 = reskv.map(c => Array(c(0).slice(15, 15 + 20), c(1).slice(59, 59 + 1), c(1).slice(60, 60 + 2), c(1).slice(62, 62 + 6), c(1).slice(68, 68 + 18), getHours(c(0).slice(0, 0 + 13), c(1).slice(0, 0 + 13)))).map { case Array(s0, s1, s2, s3, s4, s5) => ResultSchema2(s0, s1, s2, s3, s4, s5.toFloat) }.toDF()

    reskv.unpersist()

    restb1.registerTempTable("Res1Table")
    restb2.registerTempTable("Res2Table")

    val totalTable = sqlContext.sql("Select Res1Table.PKGNO, Res1Table.STATUSA2, Res1Table.STATUSB1, Res1Table.STATUSB2, Res1Table.PROC_DATEA, Res1Table.PROC_TIMEA, Res1Table.PROC_DTA, Res1Table.PROC_BRHA, Res1Table.PROC_DATEB, Res1Table.PROC_TIMEB, Res1Table.PROC_DTB, Res1Table.PROC_BRHB, Res1Table.PKGVALUE, Res1Table.PKGCOLL, Res1Table.PKGWEIGHT, Res1Table.PKGPOST, Res1Table.CONTRACTNO1, Res1Table.CONTRACTNO2, Res1Table.DESTZIPCODE, Res2Table.PROC_H, Res2Table.REASON, Res2Table.REC_BRH, Res2Table.REC_BRH_TEL, Res2Table.HOURS, Res1Table.DESTZIPCODE_LEN, Res1Table.PROC_YMA, Res1Table.PROC_YMB from Res1Table join Res2Table where Res1Table.PKGNO = Res2Table.PKGNO")

    totalTable.cache()
    totalTable.registerTempTable("TotalTable")

    val tbpkg = sqlContext.sql("Select a.PKGNO, a.STATUSA2, a.STATUSB1, a.STATUSB2, a.PROC_DATEA, a.PROC_TIMEA, a.PROC_DTA, a.PROC_BRHA, a.PROC_DATEB, a.PROC_TIMEB, a.PROC_DTB, a.PROC_BRHB, a.PKGVALUE, a.PKGCOLL, a.PKGWEIGHT, a.PKGPOST, a.CONTRACTNO1, a.CONTRACTNO2, a.DESTZIPCODE, a.PROC_H, a.REASON, a.REC_BRH, a.REC_BRH_TEL, a.HOURS from TotalTable a")
    tbpkg.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:/home/cloudera/atbpkg01.csv")

    val tbpkgD1 = sqlContext.sql("select a.PROC_DATEA,a.PROC_YMA,substring(a.PROC_BRHA,1,3) as PROC_CHGBRHA,a.PROC_BRHA,count(a.PKGNO) as PKG_CNT,sum( case when a.HOURS <= 24 then 1 else 0 end ) as PKG_CNTJ0,sum( case when a.HOURS > 24 and a.HOURS <= 48 then 1 else 0 end ) as PKG_CNTJ1,sum( case when a.HOURS > 48 and a.HOURS <= 72 then 1 else 0 end ) as PKG_CNTJ2,sum( case when a.HOURS > 72 and a.HOURS <= 96 then 1 else 0 end ) as PKG_CNTJ3,sum( case when a.HOURS > 96 then 1 else 0 end ) as PKG_CNTJX,sum( case when a.HOURS <= 24 then a.HOURS else 0 end ) as PKG_HRSJ0,sum( case when a.HOURS > 24 and a.HOURS <= 48 then a.HOURS else 0 end ) as PKG_HRSJ1,sum( case when a.HOURS > 48 and a.HOURS <= 72 then a.HOURS else 0 end ) as PKG_HRSJ2,sum( case when a.HOURS > 72 and a.HOURS <= 96 then a.HOURS else 0 end ) as PKG_HRSJ3,sum( case when a.HOURS > 96 then a.HOURS else 0 end ) as PKG_HRSJX,count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH,count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI,sum(a.HOURS) as PKG_HRS,sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH,sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by a.PROC_DATEA,a.PROC_YMA,substring(a.PROC_BRHA,1,3),a.PROC_BRHA")
    tbpkgD1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:/home/cloudera/atbpkgD1.csv")

    val tbpkgD2 = sqlContext.sql("select a.PROC_DATEB ,a.PROC_YMB, substring(a.PROC_BRHB,1,3) as PROC_CHGBRHB, a.PROC_BRHB ,count(a.PKGNO) as PKG_CNT, sum( case when a.HOURS <= 24 then 1 else 0 end ) as PKG_CNTJ0, sum( case when a.HOURS >  24 and a.HOURS <= 48 then 1 else 0 end ) as PKG_CNTJ1, sum( case when a.HOURS >  48 and a.HOURS <= 72 then 1 else 0 end ) as PKG_CNTJ2, sum( case when a.HOURS >  72 and a.HOURS <= 96 then 1 else 0 end ) as PKG_CNTJ3, sum( case when a.HOURS >  96 then 1 else 0 end ) as PKG_CNTJX, sum( case when a.HOURS <= 24 then a.HOURS else 0 end ) as PKG_HRSJ0, sum( case when a.HOURS >  24 and a.HOURS <= 48 then a.HOURS else 0 end ) as PKG_HRSJ1, sum( case when a.HOURS >  48 and a.HOURS <= 72 then a.HOURS else 0 end ) as PKG_HRSJ2, sum( case when a.HOURS >  72 and a.HOURS <= 96 then a.HOURS else 0 end ) as PKG_HRSJ3, sum( case when a.HOURS >  96 then a.HOURS else 0 end ) as PKG_HRSJX, count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH, count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI, sum(a.HOURS) as PKG_HRS, sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH, sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by a.PROC_DATEB ,a.PROC_YMB, substring(a.PROC_BRHB,1,3), a.PROC_BRHB")
    tbpkgD2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:/home/cloudera/atbpkgD2.csv")

    val tbpkgH = sqlContext.sql("select (case when a.STATUSB1=\"H\" then \"Undone\" else \"Done\" end) as STATUSB_DESC, a.STATUSB2 as STATUSB2, substring(a.PROC_TIMEB,1,2) as PROC_BHH, a.PROC_DATEA, a.PROC_YMA, substring(a.PROC_BRHA,1,3) as PROC_CHGBRHA, a.PROC_BRHA, a.PROC_DATEB, a.PROC_YMB, substring(a.PROC_BRHB,1,3) as PROC_CHGBRHB, a.PROC_BRHB, (case when a.DESTZIPCODE_LEN < 3 then \"NA\" else substring(a.DESTZIPCODE,1,3) end) as DESTZIPCODE3, (case when a.HOURS <= 24 then \"J+0D\" when a.HOURS > 24 and a.HOURS <= 48 then \"J+1D\" when a.HOURS > 48 and a.HOURS <= 72 then \"J+2D\" when a.HOURS > 72 and a.HOURS <= 96 then \"J+3D\" when a.HOURS > 96 then \"J+3D above\" else \"NA\" end) as PROCDURGROUP, count(a.PKGNO) as PKG_CNT, count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH, count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI, sum(a.HOURS) as PKG_HRS, sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH, sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by (case when a.STATUSB1=\"H\" then \"Undone\" else \"Done\" end), a.STATUSB2, substring(a.PROC_TIMEB,1,2), a.PROC_DATEA, a.PROC_YMA, substring(a.PROC_BRHA,1,3), a.PROC_BRHA, a.PROC_DATEB, a.PROC_YMB, substring(a.PROC_BRHB,1,3), a.PROC_BRHB, (case when DESTZIPCODE_LEN < 3 then \"NA\" else substring(a.DESTZIPCODE,1,3) end), (case when a.HOURS <= 24 then \"J+0D\" when a.HOURS >  24 and a.HOURS <= 48 then \"J+1D\" when a.HOURS > 48 and a.HOURS <= 72 then \"J+2D\" when a.HOURS >  72 and a.HOURS <= 96 then \"J+3D\" when a.HOURS >  96 then  \"J+3D above\" else \"NA\" end)")
    tbpkgH.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("file:/home/cloudera/atbpkgH.csv")



  }}



# 中華郵政            投遞郵寄資料分析


```python
程式:考題提供 post_office_poc.scala
硬體:intel I7 16G RAM
軟體:
VMware-player-7.0.0-2305329
Coudera 5.12.0-0
SPARK SBT 使用版本
version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
```

# 1.1 宣告CASE類別


```python
程序說明:
Step 1.宣告CASE類別 ResultSchema1 並定義有哪些參數 預作使用SCALA中可以使用SQL TABLE的準備
Step 2.宣告CASE類別 ResultSchema2 並定義有哪些參數 預作使用SCALA中可以使用SQL TABLE的準備


規則說明:
case class ResultSchema1(PKGNO:String, STATUSA2:String, STATUSB1:String, STATUSB2:String, PROC_DATEA:String, PROC_TIMEA:String, 
PROC_DTA:String, PROC_BRHA:String, PROC_DATEB:String, PROC_TIMEB:String, PROC_DTB:String, PROC_BRHB:String, PKGVALUE:String, 
PKGCOLL:String, PKGWEIGHT:String, PKGPOST:String, CONTRACTNO1:String, CONTRACTNO2:String, DESTZIPCODE:String, 
DESTZIPCODE_LEN:Int, PROC_YMA:String, PROC_YMB:String)
//宣告ResultSchema1 類別,並分別定義22個參數命名與型別皆為字串

案例
依照case class 形成SCHEMA1
+-----+--------+--------+--------+----------+----------+--------+---------+----------+----------+--------+---------+--------+---
|PKGNO|STATUSA2|STATUSB1|STATUSB2|PROC_DATEA|PROC_TIMEA|PROC_DTA|PROC_BRHA|PROC_DATEB|PROC_TIMEB|PROC_DTB|PROC_BRHB|PKGVALUE|PKG
+-----+--------+--------+--------+----------+----------+--------+---------+----------+----------+--------+---------+--------+---


----+---------+-------+-----------+-----------+-----------+---------------+--------+--------+
COLL|PKGWEIGHT|PKGPOST|CONTRACTNO1|CONTRACTNO2|DESTZIPCODE|DESTZIPCODE_LEN|PROC_YMA|PROC_YMB|

規則說明:
case class ResultSchema2(PKGNO:String, PROC_H:String, REASON:String, REC_BRH:String, REC_BRH_TEL:String, HOURS:Float)
//宣告ResultSchema2 類別分別定義6個參數命名與型別為字串與浮點數.

案例
依照case class 形成SCHEMA2
+--------------------+------+------+-------+-----------+-----+
|               PKGNO|PROC_H|REASON|REC_BRH|REC_BRH_TEL|HOURS|
+--------------------+------+------+-------+-----------+-----+

```

# 1.2  宣告函數

程序說明:

Step 1.定義gettimeStamp()取得時間戳記
Step 2.定義getSortValue()將資料排序
Step 3.定義getRecord()取得資料並去空值
Step 4 定義getHours()取得時間並轉換成小時為單位



```python

規則說明:
def getTimeStamp(s:String):Timestamp={
//定義取得時間戳記方法,輸入變數字串回傳Timestamp,

  val format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
  //宣告變數format 並實作SimpleDateFormat方法
    
  val d = format.parse(s)
  //宣告變數 d  並傳入字串s後使用parse方法轉換格式
    
  val t = new Timestamp(d.getTime())
  //宣告變數 t  並實作Timestamp帶入變數d.getTime()方法取得時間字串
    
  return t
  
  //回傳 變數t 的Timestamp 值
  
    }
    
案例   帶入一筆時間資料轉換出時間戳記

  轉換前時間資料     2016-10-0208:32:00  
  轉換後時間戳記     1475422320000    
    

規則說明:
def getSortValue(v:Iterable[String]):String={val l = v.toList.sorted; return l(0)}
    //將資料轉成List型別後排序後回傳




規則說明:
def getRecord(r:(String, List[String])):Option[List[String]]={
    //定義傳入函數參數為(Key,Value)型態分別為字串與串列 回傳值為Option
      
    val res = r._2.find(s => s.slice(0, 0+13) > r._1.slice(0, 0+13))
    //將傳入參數List(r1,r2)中以r2做slice(0, 0+13)所得之時間戳記，與r1做slice(0, 0+13)所得之時間戳記比較
    //若較大則回傳值 
    
    if (res.isDefined) {Some(List(r._1, res.get))} else None
    //若res有回傳值則 回傳函數的 Option為串列List(r1,r2)資料若無則回傳空值
    }
    
    
    
案例  帶入一筆原始資料函數處理如下 

處理前紀錄格式(Key,Value)=(K1,List(V1,V2))
(1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  ,List(1475436998000H482562900103070      2016-10-0212:36:38880580012 , 1475449019000I482562900103070      2016-10-0215:56:59880580 )))17/11/19 10:17:15 INFO SparkContext: Invoking stop() from shutdown hook


處理後紀錄格式為List(V1,V2)
List(1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  , 1475436998000H482562900103070      2016-10-0212:36:38880580012 )17/11/19 09:55:42 INFO SparkContext: Invoking stop() from shutdown hook
    
    
    
規則說明:
def getHours(b:String, e:String):String={ val h = "%.2f".format((e.toDouble-b.toDouble)/1000/3600); return h }
//傳入兩個時間戳記字串並轉換成浮點數相減後得出的時間差再轉換成小時為單位並回傳


案例  帶入一筆原始資料函數處理如下 
  2016-10-02 08:32:00   
  2016-10-02 12:36:38
 
  轉換後成時戳記      
  1475422320000
  1475436998000
  兩者的差/1000/3600轉換成小時為單位即為4.08小時
  






```

# 1.3  讀取檔案資料

程序說明	

Step1.讀入所有資料若符合pkgCodeLst條件將資料存入 val a
Step2.過濾val a 中紀錄字首為A之資料並計算其時間戳記,並以郵件編號為鍵值groupBY同一筆紀錄後存入val akv
Step3.過濾val a 中紀錄字首為H或I之資料並計算其時間戳記,並以郵件編號為鍵值groupBY同一筆紀錄後存入val hikv


```python

規則說明:
val a = sc.textFile(inDir).filter(r => pkgCodeLst.contains(r.slice(14,14+2))).cache()
//  從inDir讀取檔案 過濾[郵件狀態代碼]slice(14,14+2) 存入RDD 其中過濾條件為pkgCodeLst中的元素 

案例
I401061673003978      2016-10-0212:14:29950009  
H401061873003978      2016-10-0212:13:55950009012 
Y455731200102870      2016-10-0207:03:00500038      194                                 
I455731200102870      2016-10-0215:40:39500038                                          
Y455737600102870      2016-10-0213:10:30100250      438                                 
P556346500102870      2016-10-0208:54:54900064           

```


```python
規則說明:
val akv = a.filter(r => r.matches("^A.*")).distinct()
// 濾出首位字元H或I的紀錄  並做distinct()消去重複紀錄

.map(r => (r.slice(2,2+20), "%s%s".format(getTimeStamp(r.slice(22,22+18)).getTime().toString,r)))
//將每筆紀錄的[郵件號碼]slice(2,2+20)map成(key,value)的Key 並對[處理時間],[處理日期] slice(22,22+18)map成(key,value)的value後
//,執行getTimeStamp()取得時間戳記
 
.groupByKey().mapValues(getSortValue(_))

//最後以[郵件號碼]對(KEY,VALUE)做groupBy,且使用mapValue()將KEY-VALUE中的VALUE排序


案例
val a 原始紀錄值兩筆處理說明如下

A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116                                                            
A406093273403678      2016-10-0208:06:00600028000000000000000012000000             10603       

        
val akv (STEP)
先將 a.filter().map()形成  (K,V)=(郵件編號,時間戳記+val a原始紀錄值,再以郵件編號groupBy且以 getSortValue()排序

(82562900103070      ,1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  ) (K1,V1)
(06093273403678      ,1475420760000A406093273403678      2016-10-0208:06:00600028000000000000000012000000             10603) (K1,V2)
                
(說明): 1475353093000為getTimeStamp(2016-10-0113:18:13900056012)所計算出的時間戳記  
(說明): 此例中A郵件狀態無相同編號需要排序




```


```python
規則說明:
val hikv = a.filter(r => (r.matches("^H.*") || r.matches

("^I.*"))).distinct()
// 濾出首位字元H或I的紀錄  並做distinct()

.map(r => (r.slice(2,2+20), "%s%s".format(getTimeStamp(r.slice(22,22+18)).getTime().toString,r)))
//將每筆紀錄的郵件號碼slice(2,2+20)map成(key,value)的Key 
 將slice(22,22+18)map成(key,value)的value ,並對時間日期做getTimeStamp()取得時間戳記
  
.groupByKey).mapValues(_.toList.sorted) 

//以郵件號碼對(key,value)做groupBy,後將(key,value)中的VALUE排序


案例
val a 原始紀錄值兩筆

H425740000101170      2016-10-0113:18:13900056012                                                          
H425740000101170      2016-10-0213:24:06900056012         

val hikv (STEP1)
先將 a.filter().map()形成  (K,V)=(郵件編號,時間戳記+val a原始紀錄值)

(25740000101170,  1475353093000     H425740000101170   2016-10-0113:18:13900056012)  (K1,V1)
(25740000101170,  1475439846000     H425740000101170   2016-10-0213:24:06900056012)  (K1,V2)
                
(說明): 1475353093000為getTimeStamp(2016-10-0113:18:13900056012)所計算出的時間戳記  

val hikv (STEP2)
再groupByKey後mapValues排序

val hikv= (K1,List(V1,V2))
(25740000101170  ,List(1475353093000H425740000101170  2016-10-0113:18:13900056012 1475439846000H425740000101170 2016-10-0213:24:06900056012 ))

(說明): 此例中H郵件狀態有相同編號需要排序


```

# 1.4   將記錄轉化存入DataFrame

程序說明:

Step 1.釋放RRD記憶體
Step 2.以郵件編號為鍵值 JOIN akvRDD 與 hikvRDD 成為 reskvRDD
Step 3.
Step 4 


```python
規則說明:
a.unpersist()
//釋放 RDD persist 

規則說明:
val reskv = akv.join(hikv).mapValues(getRecord(_).getOrElse(List())).values.filter(! _.isEmpty).cache()
//

案例 將Akv與hikv做JOIN處理
 val akv 原始資料
(82562900103070      ,1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  )
(06093273403678      ,1475420760000A406093273403678      2016-10-0208:06:00600028000000000000000012000000             10603)

 val hikv 原始資料
(82562900103070      ,List(1475436998000H482562900103070      2016-10-0212:36:38880580012 , 1475449019000I482562900103070      2016-10-0215:56:59880580 ))
(06093273403678      ,List(1475430968000H406093273403678      2016-10-0210:56:08970025012   , 1475453068000I406093273403678      2016-10-0217:04:28970025  ))17/11/19 11:32:54 INFO SparkContext: Invoking stop() from shutdown hook


 Join 為 reskv結果資料
List(1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  , 1475436998000H482562900103070      2016-10-0212:36:38880580012 )
List(1475420760000A406093273403678      2016-10-0208:06:00600028000000000000000012000000             10603, 1475430968000H406093273403678      2016-10-0210:56:08970025012   )

```


```python
規則說明:
 val restb1 = reskv.map(c => Array(c(0).slice(15,15+20), c(0).slice(14,14+1), c(1).slice(13,13+1), c(1).slice(14,14+1),c(0).slice(35,35+10)+" 00:00:00",c(0).slice(45,45+8), c(0).slice(35,35+10)+" "+c(0).slice(45,45+8), c(0).slice(53,53+6), c(1).slice(35,35+10)+" 00:00:00", c(1).slice(45,45+8), c(1).slice(35,35+10)+" "+c(1).slice(45,45+8), c(1).slice(53,53+6), c(0).slice(59,59+6), c(0).slice(65,65+6), c(0).slice(71,71+6), c(0).slice(77,77+6), c(0).slice(83,83+6), c(0).slice(89,89+7), c(0).slice(96,96+5), c(0).slice(96,96+5).trim(), c(0).slice(35,35+4)+c(0).slice(40,40+2), c(1).slice(35,35+4)+c(1).slice(40,40+2))).map{case Array(s0,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21) => ResultSchema1(s0,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19.length,s20,s21)}.toDF()
 //將reskv map成Array後再map成DataFrame   
(說明)reskv為兩個List 第一次map即按c(0)第一個List與c(1)第二個List依如上規則slice出22個元素
    
案例  將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025  

先形成reskv為如下List格式      
List(1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  , 1475436998000H482562900103070      2016-10-0212:36:38880580012 )
List(1475420760000A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603, 1475453068000I406093273403678      2016-10-0217:04:28970025  )17/11/19 14:59:49 INFO MapPartitionsRDD: Removing RDD 22 from persistence list

再轉化成DataFrame
[82562900103070      ,4,H,4,2016-10-02 00:00:00,08:32:00,2016-10-02 08:32:00,600028,2016-10-02 00:00:00,12:36:38,2016-10-02 12:36:38,880580,000000,000000,000189,000065,      ,       ,116  ,3,201610,201610]
[06093273403678      ,4,I,4,2016-10-02 00:00:00,08:06:00,2016-10-02 08:06:00,730039,2016-10-02 00:00:00,17:04:28,2016-10-02 17:04:28,970025,000000,000000,000012,000000,      ,       ,10603,5,201610,201610]17/11/19 14:53:00 INFO SparkContext: Invoking stop() from shutdown hook

    
```


```python
規則說明:
val restb2 = reskv.map(c => Array(c(0).slice(15,15+20), c(1).slice(59,59+1), c(1).slice(60,60+2), c(1).slice(62,62+6),c(1).slice(68,68+18), getHours(c(0).slice(0,0+13), c(1).slice(0,0+13)))).map{case Array(s0,s1,s2,s3,s4,s5) => ResultSchema2(s0,s1,s2,s3,s4,s5.toFloat)}.toDF()
//將reskv map成Array後再map成DataFrame   
(說明)reskv為兩個List 第一次map即按c(0)第一個List與c(1)第二個List依如上規則slice出6個元素


案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025  



先形成reskv為如下List格式  
List(1475422320000A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  , 1475436998000H482562900103070      2016-10-0212:36:38880580012 )
List(1475420760000A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603, 1475453068000I406093273403678      2016-10-0217:04:28970025  )17/11/19 14:59:49 INFO MapPartitionsRDD: Removing RDD 22 from persistence list

再轉化成DataFrame
[82562900103070      ,0,12, ,,4.08]
[06093273403678      , , ,,,8.97]

```

# 1.5   將DataFrame轉存入 Spark Table

程序說明:

Step 1.釋放 reskv RDD記憶體
Step 2.restb1 DataFrame 註冊使用Spark TempTable
Step 3.restb2 DataFrame 註冊使用Spark TempTable
Step 4 變數 totalTable   註冊使用Spark TempTable


```python
規則說明:
reskv.unpersist()
規則說明:
restb1.registerTempTable("Res1Table")
restb2.registerTempTable("Res2Table")
規則說明:

val totalTable = sqlContext.sql("Select Res1Table.PKGNO, Res1Table.STATUSA2, Res1Table.STATUSB1, Res1Table.STATUSB2, Res1Table.PROC_DATEA, Res1Table.PROC_TIMEA, Res1Table.PROC_DTA, Res1Table.PROC_BRHA, Res1Table.PROC_DATEB, Res1Table.PROC_TIMEB, Res1Table.PROC_DTB, Res1Table.PROC_BRHB, Res1Table.PKGVALUE, Res1Table.PKGCOLL, Res1Table.PKGWEIGHT, Res1Table.PKGPOST, Res1Table.CONTRACTNO1, Res1Table.CONTRACTNO2, Res1Table.DESTZIPCODE, Res2Table.PROC_H, Res2Table.REASON, Res2Table.REC_BRH, Res2Table.REC_BRH_TEL, Res2Table.HOURS, Res1Table.DESTZIPCODE_LEN, Res1Table.PROC_YMA, Res1Table.PROC_YMB from Res1Table join Res2Table where Res1Table.PKGNO = Res2Table.PKGNO")


規則說明:                                
totalTable.cache()
//cache() 方法則是預設儲存位置為 StorageLevel.MEMORY_ONLY
totalTable.registerTempTable("TotalTable")
//totalTable 註冊使用Spark TempTable     
                                
案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025  
                                
                                
+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+---------------+--------+--------+
|               PKGNO|STATUSA2|STATUSB1|STATUSB2|         PROC_DATEA|PROC_TIMEA|           PROC_DTA|PROC_BRHA|         PROC_DATEB|PROC_TIMEB|           PROC_DTB|PROC_BRHB|PKGVALUE|PKGCOLL|PKGWEIGHT|PKGPOST|CONTRACTNO1|CONTRACTNO2|DESTZIPCODE|PROC_H|REASON|REC_BRH|REC_BRH_TEL|HOURS|DESTZIPCODE_LEN|PROC_YMA|PROC_YMB|
+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+---------------+--------+--------+
|06093273403678      |       4|       I|       4|2016-10-02 00:00:00|  08:06:00|2016-10-02 08:06:00|   730039|2016-10-02 00:00:00|  17:04:28|2016-10-02 17:04:28|   970025|  000000| 000000|   000012| 000000|           |           |      10603|      |      |       |           | 8.97|              5|  201610|  201610|
|82562900103070      |       4|       H|       4|2016-10-02 00:00:00|  08:32:00|2016-10-02 08:32:00|   600028|2016-10-02 00:00:00|  12:36:38|2016-10-02 12:36:38|   880580|  000000| 000000|   000189| 000065|           |           |      116  |     0|    12|       |           | 4.08|              3|  201610|  201610|
+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+---------------+--------+--------+

                                

```

# 1.6   使用Spark Table做SQL 查詢

程序說明:

Step 1.統計每一郵件從生出A狀態到H/I狀態的時間歷程與處理局號郵件所有資料
Step 2.以A局號與月份統計分析H/I狀態的處理時間分類件數
Step 3.以H/I局號與月份統計分析H/I狀態的處理時間分類件數
Step 4.以郵遞區號[DESTZIPCODE3]與遞延日[PROCDURGROUP]統計各局H/I 效率


```python
規則說明: 
val tbpkg = sqlContext.sql("Select a.PKGNO, a.STATUSA2, a.STATUSB1, a.STATUSB2, a.PROC_DATEA, a.PROC_TIMEA, a.PROC_DTA, a.PROC_BRHA, a.PROC_DATEB, a.PROC_TIMEB, a.PROC_DTB, a.PROC_BRHB, a.PKGVALUE, a.PKGCOLL, a.PKGWEIGHT, a.PKGPOST, a.CONTRACTNO1, a.CONTRACTNO2, a.DESTZIPCODE, a.PROC_H, a.REASON, a.REC_BRH, a.REC_BRH_TEL, a.HOURS from TotalTable a")
//由表格TotalTable 選取如下案例個欄位
tbpkg.save("%s/TBPKG".format(outDir), "com.databricks.spark.csv")
////將檔案存回outDir路徑 使用csv格式




(說明)統計每一郵件從生出A狀態到H/I狀態的時間歷程與處理局號郵件所有資料
案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025 


      [郵件編號]      [狀態碼2] [狀態碼I/H][I/H狀態碼2] [A狀態日期]      [A狀態時間] [A狀態日期時間]     [A處理局號]  [H/I狀態日期]      [H/I狀態時間] [H/I狀態日期時間][H/I處理局號][保價金額][代收金額][重量]   [郵資]                             [郵遞區號]          [原因]                [處理時間]

+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+
|               PKGNO|STATUSA2|STATUSB1|STATUSB2|         PROC_DATEA|PROC_TIMEA|           PROC_DTA|PROC_BRHA|         PROC_DATEB|PROC_TIMEB|           PROC_DTB|PROC_BRHB|PKGVALUE|PKGCOLL|PKGWEIGHT|PKGPOST|CONTRACTNO1|CONTRACTNO2|DESTZIPCODE|PROC_H|REASON|REC_BRH|REC_BRH_TEL|HOURS|
+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+
|06093273403678      |       4|       I|       4|2016-10-02 00:00:00|  08:06:00|2016-10-02 08:06:00|   730039|2016-10-02 00:00:00|  17:04:28|2016-10-02 17:04:28|   970025|  000000| 000000|   000012| 000000|           |           |      10603|      |      |       |           | 8.97|
|82562900103070      |       4|       H|       4|2016-10-02 00:00:00|  08:32:00|2016-10-02 08:32:00|   600028|2016-10-02 00:00:00|  12:36:38|2016-10-02 12:36:38|   880580|  000000| 000000|   000189| 000065|           |           |      116  |     0|    12|       |           | 4.08|
+--------------------+--------+--------+--------+-------------------+----------+-------------------+---------+-------------------+----------+-------------------+---------+--------+-------+---------+-------+-----------+-----------+-----------+------+------+-------+-----------+-----+


```


```python
規則說明: 
val tbpkgD1 = sqlContext.sql("select a.PROC_DATEA,a.PROC_YMA,substring(a.PROC_BRHA,1,3) as PROC_CHGBRHA,a.PROC_BRHA,count(a.PKGNO) as PKG_CNT,sum( case when a.HOURS <= 24 then 1 else 0 end ) as PKG_CNTJ0,sum( case when a.HOURS > 24 and a.HOURS <= 48 then 1 else 0 end ) as PKG_CNTJ1,sum( case when a.HOURS > 48 and a.HOURS <= 72 then 1 else 0 end ) as PKG_CNTJ2,sum( case when a.HOURS > 72 and a.HOURS <= 96 then 1 else 0 end ) as PKG_CNTJ3,sum( case when a.HOURS > 96 then 1 else 0 end ) as PKG_CNTJX,sum( case when a.HOURS <= 24 then a.HOURS else 0 end ) as PKG_HRSJ0,sum( case when a.HOURS > 24 and a.HOURS <= 48 then a.HOURS else 0 end ) as PKG_HRSJ1,sum( case when a.HOURS > 48 and a.HOURS <= 72 then a.HOURS else 0 end ) as PKG_HRSJ2,sum( case when a.HOURS > 72 and a.HOURS <= 96 then a.HOURS else 0 end ) as PKG_HRSJ3,sum( case when a.HOURS > 96 then a.HOURS else 0 end ) as PKG_HRSJX,count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH,count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI,sum(a.HOURS) as PKG_HRS,sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH,sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by a.PROC_DATEA,a.PROC_YMA,substring(a.PROC_BRHA,1,3),a.PROC_BRHA")

規則說明: 
tbpkgD1.save("%s/TBPKG_D1".format(outDir), "com.databricks.spark.csv")
//將檔案存回outDir路徑 使用csv格式

(說明)以A局號與月份統計分析H/I狀態的處理時間分類件數


案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025 


     [A狀態日期時間][狀態月份][處理局號前三碼][處理局號][件數][區間JO件數][區間J1件數][時區間J2件數][區間J3件數][區間JX件數][區間JO總時間][區間JO總時間][區間J2總時間][區間J3總時間][區間JX總時間][H件數][I件數][加總HI處理時間]    [H總處理時間]  [I總處理時間]

+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+
|         PROC_DATEA|PROC_YMA|PROC_CHGBRHA|PROC_BRHA|PKG_CNT|PKG_CNTJ0|PKG_CNTJ1|PKG_CNTJ2|PKG_CNTJ3|PKG_CNTJX|        PKG_HRSJ0|PKG_HRSJ1|PKG_HRSJ2|PKG_HRSJ3|PKG_HRSJX|PKG_CNTH|PKG_CNTI|          PKG_HRS|         PKG_HRSH|         PKG_HRSI|
+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+
|2016-10-02 00:00:00|  201610|         600|   600028|      1|        1|        0|        0|        0|        0|4.079999923706055|      0.0|      0.0|      0.0|      0.0|       1|       0|4.079999923706055|4.079999923706055|              0.0|
|2016-10-02 00:00:00|  201610|         730|   730039|      1|        1|        0|        0|        0|        0|8.970000267028809|      0.0|      0.0|      0.0|      0.0|       0|       1|8.970000267028809|              0.0|8.970000267028809|
+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+


```


```python

規則說明: 
val tbpkgD2 = sqlContext.sql("select a.PROC_DATEB ,a.PROC_YMB, substring(a.PROC_BRHB,1,3) as PROC_CHGBRHB, a.PROC_BRHB ,count(a.PKGNO) as PKG_CNT, sum( case when a.HOURS <= 24 then 1 else 0 end ) as PKG_CNTJ0, sum( case when a.HOURS >  24 and a.HOURS <= 48 then 1 else 0 end ) as PKG_CNTJ1, sum( case when a.HOURS >  48 and a.HOURS <= 72 then 1 else 0 end ) as PKG_CNTJ2, sum( case when a.HOURS >  72 and a.HOURS <= 96 then 1 else 0 end ) as PKG_CNTJ3, sum( case when a.HOURS >  96 then 1 else 0 end ) as PKG_CNTJX, sum( case when a.HOURS <= 24 then a.HOURS else 0 end ) as PKG_HRSJ0, sum( case when a.HOURS >  24 and a.HOURS <= 48 then a.HOURS else 0 end ) as PKG_HRSJ1, sum( case when a.HOURS >  48 and a.HOURS <= 72 then a.HOURS else 0 end ) as PKG_HRSJ2, sum( case when a.HOURS >  72 and a.HOURS <= 96 then a.HOURS else 0 end ) as PKG_HRSJ3, sum( case when a.HOURS >  96 then a.HOURS else 0 end ) as PKG_HRSJX, count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH, count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI, sum(a.HOURS) as PKG_HRS, sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH, sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by a.PROC_DATEB ,a.PROC_YMB, substring(a.PROC_BRHB,1,3), a.PROC_BRHB")

規則說明: 
tbpkgD2.save("%s/TBPKG_D2".format(outDir), "com.databricks.spark.csv")
//將檔案存回outDir路徑 使用csv格式




(說明)以H/I局號與月份統計分析H/I狀態的處理時間分類件數
案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025 
    [H/I狀態日期]   [狀態月份] [處理局號前三碼][處理局號][區間JO件數][區間J1件數][時區間J2件數][區間J3件數][區間JX件數][區間JO總時間][區間JO總時間][區間J2總時間][區間J3總時間][區間JX總時間][H件數][I件數]  [加總HI處理時間]    [H總處理時間]  [I總處理時間]

+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+
|         PROC_DATEB|PROC_YMB|PROC_CHGBRHB|PROC_BRHB|PKG_CNT|PKG_CNTJ0|PKG_CNTJ1|PKG_CNTJ2|PKG_CNTJ3|PKG_CNTJX|        PKG_HRSJ0|PKG_HRSJ1|PKG_HRSJ2|PKG_HRSJ3|PKG_HRSJX|PKG_CNTH|PKG_CNTI|          PKG_HRS|         PKG_HRSH|         PKG_HRSI|
+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+
|2016-10-02 00:00:00|  201610|         970|   970025|      1|        1|        0|        0|        0|        0|8.970000267028809|      0.0|      0.0|      0.0|      0.0|       0|       1|8.970000267028809|              0.0|8.970000267028809|
|2016-10-02 00:00:00|  201610|         880|   880580|      1|        1|        0|        0|        0|        0|4.079999923706055|      0.0|      0.0|      0.0|      0.0|       1|       0|4.079999923706055|4.079999923706055|              0.0|
+-------------------+--------+------------+---------+-------+---------+---------+---------+---------+---------+-----------------+---------+---------+---------+---------+--------+--------+-----------------+-----------------+-----------------+

```


```python

規則說明: 
val tbpkgH = sqlContext.sql("select (case when a.STATUSB1=\"H\" then \"Undone\" else \"Done\" end) as STATUSB_DESC, a.STATUSB2 as STATUSB2, substring(a.PROC_TIMEB,1,2) as PROC_BHH, a.PROC_DATEA, a.PROC_YMA, substring(a.PROC_BRHA,1,3) as PROC_CHGBRHA, a.PROC_BRHA, a.PROC_DATEB, a.PROC_YMB, substring(a.PROC_BRHB,1,3) as PROC_CHGBRHB, a.PROC_BRHB, (case when a.DESTZIPCODE_LEN < 3 then \"NA\" else substring(a.DESTZIPCODE,1,3) end) as DESTZIPCODE3, (case when a.HOURS <= 24 then \"J+0D\" when a.HOURS > 24 and a.HOURS <= 48 then \"J+1D\" when a.HOURS > 48 and a.HOURS <= 72 then \"J+2D\" when a.HOURS > 72 and a.HOURS <= 96 then \"J+3D\" when a.HOURS > 96 then \"J+3D above\" else \"NA\" end) as PROCDURGROUP, count(a.PKGNO) as PKG_CNT, count( case when a.STATUSB1=\"H\" then a.PKGNO else null end ) as PKG_CNTH, count( case when a.STATUSB1=\"I\" then a.PKGNO else null end ) as PKG_CNTI, sum(a.HOURS) as PKG_HRS, sum( case when a.STATUSB1=\"H\" then a.HOURS else 0 end ) as PKG_HRSH, sum( case when a.STATUSB1=\"I\" then a.HOURS else 0 end ) as PKG_HRSI from TotalTable a group by (case when a.STATUSB1=\"H\" then \"Undone\" else \"Done\" end), a.STATUSB2, substring(a.PROC_TIMEB,1,2), a.PROC_DATEA, a.PROC_YMA, substring(a.PROC_BRHA,1,3), a.PROC_BRHA, a.PROC_DATEB, a.PROC_YMB, substring(a.PROC_BRHB,1,3), a.PROC_BRHB, (case when DESTZIPCODE_LEN < 3 then \"NA\" else substring(a.DESTZIPCODE,1,3) end), (case when a.HOURS <= 24 then \"J+0D\" when a.HOURS >  24 and a.HOURS <= 48 then \"J+1D\" when a.HOURS > 48 and a.HOURS <= 72 then \"J+2D\" when a.HOURS >  72 and a.HOURS <= 96 then \"J+3D\" when a.HOURS >  96 then  \"J+3D above\" else \"NA\" end)")

規則說明: 
tbpkgH.save("%s/TBPKG_H".format(outDir), "com.databricks.spark.csv")
////將檔案存回outDir路徑 使用csv格式

(說明)以郵遞區號[DESTZIPCODE3]與遞延日[PROCDURGROUP]統計各局H/I 效率

案例 將4筆資料執行處理結果如下
A482562900103070      2016-10-0208:32:00600028000000000000000189000065             116  
A406093273403678      2016-10-0208:06:00730039000000000000000012000000             10603
H482562900103070      2016-10-0212:36:38880580012 
I406093273403678      2016-10-0217:04:28970025 
 [狀態碼意義]  [狀態碼]            [A狀態日期時間]    [狀態月份] [處理局號前三碼][處理局號]

+------------+--------+--------+-------------------+--------+------------+---------+-------------------+--------+------------+---------+------------+------------+-------+--------+--------+-----------------+-----------------+-----------------+
|STATUSB_DESC|STATUSB2|PROC_BHH|         PROC_DATEA|PROC_YMA|PROC_CHGBRHA|PROC_BRHA|         PROC_DATEB|PROC_YMB|PROC_CHGBRHB|PROC_BRHB|DESTZIPCODE3|PROCDURGROUP|PKG_CNT|PKG_CNTH|PKG_CNTI|          PKG_HRS|         PKG_HRSH|         PKG_HRSI|
+------------+--------+--------+-------------------+--------+------------+---------+-------------------+--------+------------+---------+------------+------------+-------+--------+--------+-----------------+-----------------+-----------------+
|        Done|       4|      17|2016-10-02 00:00:00|  201610|         730|   730039|2016-10-02 00:00:00|  201610|         970|   970025|         106|        J+0D|      1|       0|       1|8.970000267028809|              0.0|8.970000267028809|
|      Undone|       4|      12|2016-10-02 00:00:00|  201610|         600|   600028|2016-10-02 00:00:00|  201610|         880|   880580|         116|        J+0D|      1|       1|       0|4.079999923706055|4.079999923706055|              0.0|
+------------+--------+--------+-------------------+--------+------------+---------+-------------------+--------+------------+---------+------------+------------+-------+--------+--------+-----------------+-----------------+-----------------+

```
