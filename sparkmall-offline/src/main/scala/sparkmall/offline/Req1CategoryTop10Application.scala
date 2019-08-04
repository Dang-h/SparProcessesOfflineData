package sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import Util.StringUtil
import module.CategoryTop10
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1CategoryTop10Application {
	def main(args: Array[String]): Unit = {

		val conf: SparkConf = new SparkConf().setAppName("CategoryTop10Application").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//TODO 1 获取原始数据
		//日志中，第7列（click_category_id），第9列(order_category_id)，第11列(pay_category_id)为目标列
		val lineDataRDD: RDD[String] = sc.textFile("input/user_visit_action.csv")

		//TODO 2 转换数据结构 （click_category_id-click，1）
		val mapRDD: RDD[Array[(String, Long)]] = lineDataRDD.map { line =>
			val datas: Array[String] = line.split(",")
			//点击
			if (datas(6) != "-1") {
				Array((datas(6) + "-click", 1L))
			} else if (StringUtil.isNotEmpty(datas(8))) {
				//下单
				val categoryOrder: Array[String] = datas(8).split("-")
				categoryOrder.map {
					ids => (ids + "-order", 1L)
				}
			} else if (StringUtil.isNotEmpty(datas(10))) {
				//支付
				val categoryPay: Array[String] = datas(8).split("-")
				categoryPay.map {
					ids => (ids + "-pay", 1L)
				}
			} else {
				Array(("", 0L))
			}
		}
		//Array==>tuple
		val flatMapRDD: RDD[(String, Long)] = mapRDD.flatMap(a => a)
		//过滤无效数据（"",0L）
		val filterRDD: RDD[(String, Long)] = flatMapRDD.filter {
			case (key, vale) => {
				StringUtil.isNotEmpty(key)
			}
		}

		//TODO 3 转换后的数据分组聚合 （category-click，1） ==> （category-click，sum）
		val reduceRDD: RDD[(String, Long)] = filterRDD.reduceByKey(_ + _)

		//TODO 4 聚合后数据转换结构  （category-click，sum））==> (category,(click,sum))
		val mapRDD1: RDD[(String, (String, Long))] = reduceRDD.map {
			case (key, sum) => {
				val keys: Array[String] = key.split("-")
				(keys(0), (keys(1), sum))
			}
		}

		//TODO 5 分组转换为可迭代对象==>(category,Interator((click, sum)))
		val groupRDD: RDD[(String, Iterable[(String, Long)])] = mapRDD1.groupByKey()

		//TODO 6 分组数据转换为样例类,把一个数据转换成一个对象
		val taskId: String = UUID.randomUUID().toString

		val classRDD: RDD[CategoryTop10] = groupRDD.map {
			case (categoryId, iter) => {
				val map: Map[String, Long] = iter.toMap
				CategoryTop10(taskId, categoryId, map.getOrElse("click", 0L), map.getOrElse("order", 0L), map.getOrElse("pay", 0L))
			}
		}

		//TODO 7 转换后数据按降序排列，取前10
		//排序字段过多，采用sortWith，sortWith需要传一个Array
		val collectToArray: Array[CategoryTop10] = classRDD.collect()

		val top10Array: Array[CategoryTop10] = collectToArray.sortWith {
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
//		top10Array.foreach(println)

		//TODO 8 结果存MySQL

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

		sc.stop()
	}


}

