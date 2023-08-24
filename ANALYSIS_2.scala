import org.apache.spark.SparkContext
import scala.math.sqrt
import scala.util.Random
import java.math.RoundingMode
import java.io._

object ANALYSIS_2 {

  val sc = new SparkContext("local", "ANALYSIS")
  val path = "C://Users//Venu Pulagam//Desktop//laptop_data.csv"
  var RDD = sc.textFile(path)
  val Head = sc.parallelize(RDD.take(1))
  RDD = RDD.subtract(Head)

  def highest_sales () : (Array[String], Array[String], Array[Int]) = {
    val rd2 = RDD.map(line => {
      val field = line.split(",")
      ((field(0), field(1)), field(12).toInt)
    }).groupByKey

    val rdx = rd2.map { case (x, y) => x }

    val rd3 = rd2.map { case (x, y) => y.toArray }.map(x => x.sum).zip(rdx).sortByKey(false)
    val rd4 = rd3.map { case (x, y) => (y, x) }
    val brand = rd4.map{ case((x,y),z) => x}.collect
    val model = rd4.map{ case((x,y),z) => y}.collect
    val sales = rd4.map{ case((x,y),z) => z}.collect

    return (brand,model,sales)
  }

  def core_stats_prices_sales () : (Array[String], Array[Int], Array[Double], Array[Double]) = {
    val Rd1 = RDD.map(line => {
      val field = line.split(",")
      (field(0), (field(10).toInt, field(12).toInt))
    })

    // y - price, z - no.of sales

    val Rd2 = Rd1.map { case (x, (y, z)) => (x, z) }.groupByKey

    val Rdx = Rd2.map { case (x, y) => x }

    val Rd3 = Rd2.map { case (x, y) => y.toArray }

    val meanRDD = Rd3.map(x => x.sum / x.size)


    val medianRDD = Rd3.map { array =>
      val sortedArray = array.sorted
      val count = sortedArray.length

      val evenLength = count % 2 == 0
      val midIndex = (count - 1) / 2

      val median = sortedArray.filter(_ => evenLength).slice(midIndex, midIndex + 2).sum / 2.0
      sortedArray.filter(_ => !evenLength).apply(midIndex).toDouble

      median
    }

    val stdDevRDD = Rd3.map { array =>
      val mean = array.sum.toDouble / array.length
      val squaredDiffs = array.map(value => math.pow(value - mean, 2))
      val variance = squaredDiffs.sum / array.length
      math.sqrt(variance)
    }

    val Rd4 = Rdx.zip(meanRDD)
    val Rd5 = Rd4.zip(medianRDD)
    val Rd6 = Rd5.zip(stdDevRDD)

    val comp = Rd6.map{case (((x,y),z),a) => x}.collect
    val mean = Rd6.map{case (((x,y),z),a) => y}.collect
    val median = Rd6.map{case (((x,y),z),a) => z}.collect
    val std = Rd6.map{case (((x,y),z),a) => a}.collect

    return (comp, mean, median, std)
  }

  def avg_cost_resol () : (Array[String], Array[String], Array[Int]) = {
    val RDD2 = RDD.map(line => {
      val field = line.split(",")
      ((field(0), field(3)), field(10).toInt)
    }).groupByKey

    val case1 = RDD2.map { case (x, y) => x }

    val case2 = RDD2.map { case (x, y) => y.toArray }.map(x => x.sum / x.length).zip(case1)

    val case3 = case2.map { case (x, y) => (y, x) }

    val comp = case3.map { case ((x, y), z) => x }.collect
    val resol = case3.map { case ((x, y), z) => y }.collect
    val price = case3.map { case ((x, y), z) => z }.collect

    return(comp, resol, price)

  }

  def max_min_comp () : (Array[String], Array[String], Array[Int], Array[Int]) = {
    val RD2 = RDD.map(line => {
      val field = line.split(",")
      ((field(0), field(11)), field(10).toInt)
    }).groupByKey()

    val Case1 = RD2.map { case (x, y) => x }

    val Case2 = RD2.map { case (x, y) => y.toArray }.map(x => x.max)
    val Case3 = RD2.map { case (x, y) => y.toArray }.map(x => x.min).zip(Case2).map { case (x, y) => (y, x) }
    val Case4 = Case3.zip(Case1).map { case (x, y) => (y, x) }.sortByKey(false)

    val comp = Case4.map{ case((x,y),(a,b)) => x}.collect()
    val year = Case4.map{ case((x,y),(a,b)) => y}.collect()
    val max = Case4.map{ case((x,y),(a,b)) => a}.collect()
    val min = Case4.map{ case((x,y),(a,b)) => b}.collect()

    return (comp, year, max, min)
  }

  // FORMULA USED : Next Element = random.nextGaussian() * standardDeviation + mean

  def next_revenue () : (Array[String], Array[String]) = {
    val REV1 = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(0), (fields(10).toDouble, fields(12).toInt))
    })

    val REV2 = REV1.map { case (x, (y, z)) => (x, (y * z)) }.groupByKey().sortByKey(false)

    val REV3 = REV2.mapValues(x => (x.sum / x.size).toDouble)

    val REV4 = REV2.join(REV3).sortByKey(false)

    val REV5 = REV4.map { case (x, y) => y }.map { case (x, y) => x.map(x => math.pow(x - y, 2)) }


    val REV6 = REV5.map { x => (x.sum / x.size) }.map(x => math.sqrt(x))

    val REV7 = REV6.map(x => BigDecimal(x)).zip(REV3).map { case (x, (y, z)) => (y, (z, x)) }

    val random = new Random()
    val rand = random.nextGaussian()

    val REV8 = REV7.map { case (x, (y, z)) => (x, ((rand*0.5)+y)) }

    val REV_NEXT = REV8.map { case (x, y) => (x, "%.2f".format(y)) }

    val REV_NAME = REV_NEXT.map{ case(x,y) => x}.collect()
    val REV_AMOUNT = REV_NEXT.map{ case(x,y) => y}.collect()

    return (REV_NAME, REV_AMOUNT)

  }

  def price_comp () : (Array[String], Array[Double],
    Array[String], Array[Double],
    Array[String], Array[Double],
    Array[String], Array[Double],
    Array[String], Array[Double],
    Array[String], Array[Double],
    Array[String], Array[Double],Double) = {

    val COST1 = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(10).toInt)
    })

    val net_perc = COST1.map { case (a, b, c, d, e, f, g, h) => ((a, h * 0.15), (b, h * 0.10), (c, h * 0.20), (d, h * 0.10), (e, h * 0.10), (f, h * 0.15), (g, h * 0.05)) }


    val screensize = net_perc.map { case (a, b, c, d, e, f, g) => a }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val screensize_name = screensize.map { case (x, y) => x }.collect()
    val screensize_cost = screensize.map { case (x, y) => y }.collect()


    val screenresol = net_perc.map { case (a, b, c, d, e, f, g) => b }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val screenresol_name = screenresol.map { case (x, y) => x }.collect()
    val screenresol_cost = screenresol.map { case (x, y) => y }.collect()

    val cpu = net_perc.map { case (a, b, c, d, e, f, g) => c }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val cpu_name = cpu.map { case (x, y) => x }.collect()
    val cpu_cost = cpu.map { case (x, y) => y }.collect()

    val ram = net_perc.map { case (a, b, c, d, e, f, g) => d }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val ram_name = ram.map { case (x, y) => x }.collect()
    val ram_cost = ram.map { case (x, y) => y }.collect()

    val memory = net_perc.map { case (a, b, c, d, e, f, g) => e }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val mem_name = memory.map { case (x, y) => x }.collect()
    val mem_cost = memory.map { case (x, y) => y }.collect()

    val gpu = net_perc.map { case (a, b, c, d, e, f, g) => f }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val gpu_name = gpu.map { case (x, y) => x }.collect()
    val gpu_cost = gpu.map { case (x, y) => y }.collect()

    val os = net_perc.map { case (a, b, c, d, e, f, g) => g }.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val os_name = os.map { case (x, y) => x }.collect()
    val os_cost = os.map { case (x, y) => y }.collect()

    val others = COST1.map { case (a, b, c, d, e, f, g, h) => (h * 0.15) }
    val avg = others.sum / others.count

    return (screensize_name, screensize_cost,
      screenresol_name, screenresol_cost, cpu_name,
      cpu_cost, ram_name, ram_cost, mem_name, mem_cost,
      gpu_name, gpu_cost, os_name, os_cost, avg)

  }

  def best_invest () : (Array[String], Array[String], String, String) = {

    val REV1 = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(0), (fields(10).toDouble, fields(12).toInt))
    })
    val REV2 = REV1.map { case (x, (y, z)) => (x, (y * z)) }.groupByKey().sortByKey(false)
    val REV3 = REV2.mapValues(x => (x.sum / x.size).toDouble)
    val REV4 = REV2.join(REV3).sortByKey(false)
    val REV5 = REV4.map { case (x, y) => y }.map { case (x, y) => x.map(x => math.pow(x - y, 2)) }
    val REV6 = REV5.map { x => (x.sum / x.size) }.map(x => math.sqrt(x))
    val REV7 = REV6.map(x => BigDecimal(x)).zip(REV3).map { case (x, (y, z)) => (y, (z, x)) }
    val random = new Random()

    val REV_NEXT = REV7.map { case (x, (y, z)) => (x,  random.nextGaussian()*z + y) }

    val REV_CURRENT = RDD.map(lines => {
      val fields = lines.split(",")
      ((fields(0), fields(11).toInt), fields(10).toInt, fields(12).toInt)
    }).map { case (x, y, z) => (x, BigInt(y * z)) }.groupByKey.sortByKey(false)

    val PART_YEAR = REV_CURRENT.map { case (x, z) => (x, z.sum) }.map { case ((x, y), z) => (x, (y, z)) }.groupByKey.sortByKey(false).map { case (x, y) => (x, y.toArray) }

    val REV_LATEST = PART_YEAR.map { case (x, y) => (x, y(0)) }.map { case (x, (y, z)) => (x, z) }

    val JOINED = REV_LATEST.join(REV_NEXT).sortByKey(false).map { case (x, (y, z)) => (x, (z.toDouble - y.toDouble)) }.map { case (x, y) => ("%.2f".format(y), x) }.sortByKey(false)
    val comp_profit = JOINED.map{ case(x,y) => x}.collect()
    val comp_name = JOINED.map{ case(x,y) => y}.collect()
    val ind = comp_profit.max
    val max = comp_name(comp_profit.toList.indexOf(ind))
    return ( comp_profit, comp_name, ind, max)

  }

  def os_revenue () : (Array[String], Array[String]) = {
    val OS_SALES = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(8), fields(12).toInt)
    }).groupByKey.map { case (x, y) => (x, y.sum) }

    val COST1 = RDD.map(lines => {
      val fields = lines.split(",")
      (fields(8), fields(10).toInt)
    })

    val net_perc = COST1.map { case (a, b) => (a, b * 0.05) }

    val os = net_perc.groupByKey.map { case (x, y) => (x, y.sum / y.size) }

    val OS_REV = OS_SALES.join(os).map { case (x, (y, z)) => (x, BigDecimal(y * z)) }

    val os_name = OS_REV.map{ case(x,y) => x}.collect()
    val os_revenue = OS_REV.map{ case(x,y) => "%.2f".format(y)}.collect()

    return (os_name, os_revenue)
  }

  def main(args: Array[String]) : Unit = {

    val (name1, amount1) = next_revenue()
    val (a1,a2, b1,b2, c1,c2, d1,d2, e1,e2, f1,f2, g1,g2, avg) = price_comp()
    val (profit3, name3, maxpro, max_name) = best_invest()
    val (brand4, model4, sales4) = highest_sales()
    val (comp5, mean5, median5, std5) = core_stats_prices_sales()
    val (comp6, resol6, price6) = avg_cost_resol()
    val (comp7, year7, max7, min7) = max_min_comp()
    val (os7, rev7) = os_revenue()

    val file = new File("C://Users//Venu Pulagam//Desktop//Analysis_2.txt")
    val writer = new PrintWriter(file)

    writer.write("ANALYSIS ON LAPTOP DATASET : \n \n \n")


    writer.write("ESTIMATING THE NEXT REVENUE OF A COMPANY : \n \n")

    writer.write("ESTIMATED REVENUE   |   COMPANY NAME  \n")
    writer.write("---------------------------------------- \n")

    for (x <- 0 to name1.length - 1) {
      writer.write("  " + amount1(x).toString + "     |     " + name1(x).toString )
      writer.write("\n")
    }


    writer.write("\n \nESTIMATING THE COSTS OF EACH COMPONENT OF A LAPTOP : \n \n")

    writer.write("\n \nESTIMATED COST   |   SCREEN SIZE   \n")
    writer.write("------------------------------------ \n")
    for (x <- 0 to a1.length - 1) {
      writer.write("  " + "%.2f".format(a2(x)) + "     |     " + a1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  | SCREEN RESOLUTION   \n")
    writer.write("-------------------------------------- \n")
    for (x <- 0 to b1.length - 1) {
      writer.write("  " + "%.2f".format(b2(x)) + "     |     " + b1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  |    CPU MODEL   \n")
    writer.write("------------------------------------ \n")
    for (x <- 0 to c1.length - 1) {
      writer.write("  " + "%.2f".format(c2(x)) + "     |     " + c1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  | RAM SPECIFICATION   \n")
    writer.write("-------------------------------------- \n")
    for (x <- 0 to d1.length - 1) {
      writer.write("  " + "%.2f".format(d2(x)) + "     |     " + d1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  |    MEMORY SIZE   \n")
    writer.write("-------------------------------------- \n")
    for (x <- 0 to e1.length - 1) {
      writer.write("  " + "%.2f".format(e2(x)) + "     |     " + e1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  |   GRAPHIC CARD   \n")
    writer.write("-------------------------------------- \n")
    for (x <- 0 to f1.length - 1) {
      writer.write("  " + "%.2f".format(f2(x)) + "     |     " + f1(x).toString)
      writer.write("\n")
    }

    writer.write("\n \n ESTIMATED COST  |  OPERATING SYSTEM   \n")
    writer.write("-------------------------------------- \n")
    for (x <- 0 to g1.length - 1) {
      writer.write("  " + "%.2f".format(g2(x)) + "     |     " + g1(x).toString)
      writer.write("\n")
    }

    writer.write(s"\n \n*** Each laptop is subjected to an addition of RS. $avg " +
      "allowing a company to add their brand value, design and build quality expenditures")


    writer.write("\n \nBEST CHOICE FOR AN INVESTOR : \n \n")

    writer.write(" ESTIMATED PROFIT   |   COMPANY NAME  \n")
    writer.write("---------------------------------------- \n")

    for (x <- 0 to name3.length - 1) {
      writer.write("  " + profit3(x).toString + "     |     " + name3(x).toString)
      writer.write("\n")
    }

    writer.write(s"\n \nBEST CHOICE FOR AN INVESTOR IS : $max_name")
    writer.write(s"\n \nAVERAGE PROFIT HE/SHE WOULD GET IS : $maxpro\n \n")


    writer.write("\n \nHIGHEST SALES OF A MODEL OF A COMPANY : \n \n")

    writer.write("   COMPANY   |   MODEL   |   SALES  \n")
    writer.write("---------------------------------------- \n")

    for (x <- 0 to brand4.length - 1) {
      writer.write("  " + brand4(x).toString + "   |   " + model4(x).toString + "   |   " + sales4(x).toString)
      writer.write("\n")
    }


    writer.write("\n \nCORE STATS OF SALES OF A COMPANY : \n \n")

    writer.write("   COMPANY   |   MEAN   |   MEDIAN   |  STANDARD DEVIATION   \n")
    writer.write("-------------------------------------------------------------- \n")

    for (x <- 0 to comp5.length - 1) {
      writer.write("  " + comp5(x).toString + "   |   " + mean5(x).toString + "   |   " + median5(x).toString +  "   |   " + std5(x).toString)
      writer.write("\n")
    }


    writer.write("\n \nAVERAGE COST OF SCREEN RESOLUTION OF A COMPANY : \n \n")

    writer.write("   COMPANY   |   RESOLUTION   |   AVG. COST   \n")
    writer.write("--------------------------------------------- \n")

    for (x <- 0 to comp6.length - 1) {
      writer.write("  " + comp6(x).toString + "   |   " + resol6(x).toString + "   |   " + price6(x).toString)
      writer.write("\n")
    }


    writer.write("\n \nMAXIMUM AND MINIMUM PRICES OF PRODUCT OF A COMPANY : \n \n")

    writer.write("   COMPANY   |   YEAR   |   MAXIMUM PRICE   |   MINIMUM PRICE   \n")
    writer.write("--------------------------------------------------------------- \n")

    for (x <- 0 to comp7.length - 1) {
      writer.write("  " + comp7(x).toString + "   |   " + year7(x).toString + "   |   " + max7(x).toString + "   |   " + min7(x).toString)
      writer.write("\n")
    }

    writer.write("\n \nESTIMATING THE CURRENT REVENEUES OF AN OS PROVIDER : \n \n")

    writer.write(" ESTIMATED REVENUE   |   OS PROVIDER  \n")
    writer.write("---------------------------------------- \n")

    for (x <- 0 to rev7.length - 1) {
      writer.write("  " + rev7(x).toString + "     |     " + os7(x).toString)
      writer.write("\n")
    }

    writer.close()

  }

}
