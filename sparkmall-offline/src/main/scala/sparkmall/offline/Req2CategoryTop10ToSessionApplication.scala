package sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import Util.StringUtil
import module.{CategoryTop10, CategoryTop10Session, UserVisitAction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req2CategoryTop10ToSessionApplication {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("CategoryTop10ToSessionApplication").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//TODO  获取原始数据
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

		//####################################################### 项目二 - start ########################################
		//TODO 从项目一获取品类数据，过滤日志
		//转换原始数据格式，转换为样例类
		val actionRDD: RDD[UserVisitAction] = lineDataRDD.map {
			line => {
				val datas: Array[String] = line.split(",")
				UserVisitAction(
					datas(0),
					datas(1),
					datas(2),
					datas(3).toLong,
					datas(4),
					datas(5),
					datas(6).toLong,
					datas(7).toLong,
					datas(8),
					datas(9),
					datas(10),
					datas(11),
					datas(12)
				)
			}
		}
		//从项目1获取品类Top10的id,减少传输的内容，只传输id
		val ids: List[String] = result.map {
			top => top.categoryId
		}
		//使用广播变量，使数据只在Executor中保留一份。
		val broadcastIds: Broadcast[List[String]] = sc.broadcast(ids)
		//过滤原始日志数据，只剩下品类Top10相关数据
		val filterRDD: RDD[UserVisitAction] = actionRDD.filter {
			action => {
				if (action.click_category_id == -1) {
					false
				} else {
					broadcastIds.value.contains(action.click_category_id.toString)
				}
			}
		}

		//TODO 结构转换 (category-session, 1)
		val mapRDD: RDD[(String, Long)] = filterRDD.map {
			action => {
				(action.click_category_id + "_" + action.session_id, 1L)
			}
		}

		//TODO 转换后数据聚合统计 (category-session, sum)
		val reduceRDD: RDD[(String, Long)] = mapRDD.reduceByKey(_ + _)

		//TODO 转化结构 (category,(session, sum))
		val mapRDD1: RDD[(String, (String, Long))] = reduceRDD.map {
			case (key, sum) => {
				val k: Array[String] = key.split("_")
				(k(0), (k(1), sum))
			}
		}

		//TODO 分组 (category, Iterator[session, sum])
		val groupRDD: RDD[(String, Iterable[(String, Long)])] = mapRDD1.groupByKey()

		//TODO 数据降序排列，取top10
		val resultRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
			datas.toList.sortWith {
				(left, right) => {}
					left._2 > right._2
			}.take(10)
		})

		//将数据转换为样例类
		val resultMapRDD: RDD[List[CategoryTop10Session]] = resultRDD.map {
			case (categoryId, list) => {
				list.map {
					case (sessionId, clickCount) => {
						CategoryTop10Session(taskId, categoryId, sessionId, clickCount)
					}
				}
			}
		}
		//获得一个个单一对象
		val resultRDD1: RDD[CategoryTop10Session] = resultMapRDD.flatMap(list => list)
		println(resultRDD1.count)

		//####################################################### 项目二 - end ########################################
		//TODO 数据存入MySQL
		//以分区为单位循环遍历
		resultRDD1.foreachPartition(datas =>{
			val driver = "com.mysql.jdbc.Driver"
			val url = "jdbc:mysql://localhost:3306/sparkmall"
			val userName = "root"
			val passWd = "mysql"

			Class.forName(driver)
			val connection: Connection = DriverManager.getConnection(url, userName, passWd)
			val sql = "insert into category_top10_session_count ( taskId, categoryId, sessionId, clickCount) values (?, ?, ?, ?)"
			val statement: PreparedStatement = connection.prepareStatement(sql)

			datas.foreach{
				obj => {
					statement.setObject(1, obj.taskId)
					statement.setObject(2, obj.categoryId)
					statement.setObject(3, obj.sessionId)
					statement.setObject(4, obj.clickCount)
					statement.executeUpdate()
				}
			}

			statement.close()
			connection.close()
		})



		sc.stop()

	}

}