package sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import Util.StringUtil
import module.CategoryTop10
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Req1CategoryTop10Application1 {

	def main(args: Array[String]): Unit = {

		val conf: SparkConf = new SparkConf().setAppName("CategoryTop10Application").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//1、获取点击、下单和支付数量排名前 10 的品类

		//TODO 1 获取原始数据
		val lineDataRDD: RDD[String] = sc.textFile("input/user_visit_action.csv")

		//TODO 创建累加器
		val accumulator: CategoryAccumulator = new CategoryAccumulator
		//注册累加器
		sc.register(accumulator, "Top10Accumulator")

		//TODO 使用累加器
		//根据场合对不同场合数据进行累加 ==> (category-click, sum)
		lineDataRDD.foreach {
			line => {
				//将整行数据切分
				val datas: Array[String] = line.split(",")
				if (datas(6) != "-1") {
					//click
					accumulator.add(datas(6) + "-click")
				} else if (StringUtil.isNotEmpty(datas(8))) {
					//order
					//多件商品一起下单会产生1-2-3，切分，转换格式
					val orderIds: Array[String] = datas(8).split("-")
					orderIds.map {
						id => {
							accumulator.add(id + "-order")
						}
					}
				} else if (StringUtil.isNotEmpty(datas(10))) {
					//pay
					val payIds: Array[String] = datas(10).split("-")
					payIds.map {
						id => {
							accumulator.add(id + "-pay")
						}
					}
				}
			}
		}


		//TODO 获取累加器的值
		val accmulatorValue: mutable.HashMap[String, Long] = accumulator.value

		//TODO 累加器的值分组
		//自定义分组规则：groupBy；(category-click, sum) ==> (category, (click, sum))
		val categoryToMap: Map[String, mutable.HashMap[String, Long]] = accmulatorValue.groupBy {
			case (key, sum) => {
				//split返回一个数组，这里取第一个值
				key.split("-")(0)
			}
		}

		//TODO 分组的值转换为样例类
		val taskId: String = UUID.randomUUID().toString

		val categoryTop10: immutable.Iterable[CategoryTop10] = categoryToMap.map {
			case (category, map) => {
				CategoryTop10(
					taskId,
					category,
					map.getOrElse(category + "-click", 0L),
					map.getOrElse(category + "-order", 0L),
					map.getOrElse(category + "-pay", 0L)
				)
			}
		}

		//TODO 降序排列，取前10
		//对数据排序，需要将Iterator类型转为可排序的类型
		val result: List[CategoryTop10] = categoryTop10.toList.sortWith {
			(left, right) => {
				if (left.clickCount > right.clickCount) {
					true
				} else if (left.clickCount == right.clickCount) {
					if (left.orderCount > right.orderCount) {
						true
					} else if (left.orderCount == right.orderCount) {
						left.payCount > right.payCount
					} else {
						false
					}
				} else {
					false
				}
			}
		}.take(10)
		result.foreach(println)

		//TODO 8 结果存MySQL


		/*
		val driver = "com.mysql.jdbc.Driver"
		val url = "jdbc:mysql://localhost:3306/sparkmall"
		val userName = "root"
		val passWd = "mysql"

		Class.forName(driver)
		val connection: Connection = DriverManager.getConnection(url, userName, passWd)
		val sql = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values (?, ?, ?, ?, ?)"
		val statement: PreparedStatement = connection.prepareStatement(sql)

		//top10Array是一个Array，foreach在内存中操作
		top10Array.foreach{
			obj=>{
				statement.setObject(1, obj.taskId)
				statement.setObject(2, obj.categoryId)
				statement.setObject(3, obj.clickCount)
				statement.setObject(4, obj.orderCount)
				statement.setObject(5, obj.payCount)
				statement.executeUpdate()
			}
		}
		statement.close()
		connection.close()
		 */

		sc.stop()
	}


}

//创建累加器，累加器分布式共享只写变量，可以不使用shuffle就对数据进行累加
//由于累加器只有在RDD执行Action时候才会触发，所有它存在少加或者多加的可能
//少加：RDD没有执行Action算子；多加：RDD执行多次Action算子，比如count；解决：在count前cache一下

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

	var map = new mutable.HashMap[String, Long]()

	override def isZero: Boolean = {
		map.isEmpty
	}

	override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
		new CategoryAccumulator
	}

	override def reset(): Unit = {
		map.clear()
	}

	override def add(v: String): Unit = {
		//+1 更新map的值，往下走一个
		map(v) = map.getOrElse(v, 0L) + 1
	}

	override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
		//将两个map合并==>foldLeft
		var map1 = map
		var map2 = other.value

		//转变结构为==>(category-click, sum)
		//foldLeft有两个参数列表，第一个初始值，第二个(B,(String, Long)) => B，有两个参数的函数最后返回B，一般是返回合并后的另一个map
		map = map1.foldLeft(map2) {
			(tempMap, kv) => {
				val k: String = kv._1
				val v: Long = kv._2

				tempMap(k) = tempMap.getOrElse(k, 0L) + v
				tempMap
			}
		}

	}

	override def value: mutable.HashMap[String, Long] = {
		map
	}
}