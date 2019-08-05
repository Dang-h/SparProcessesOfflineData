package sparkmall.offline

import Util.DateUtil
import module.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Rep4UserAvgDuration {
	//使用Spark RDD 统计电商平台每个页面用户的平均停留时间。
	/*
		对每个用户的一个session进行统计。如：user_id为60的用户，通过页面6点击时间和跳转到页面23点击时间得出页面6的停留时间为108秒；
		依此类推得出user_id为60的用户所去过的各页面停留时间；
		然后求出各用户的各页面停留时间
		然后对同一个pageId聚合，求出pageId个数
		最后求出此pageId用户平均停留时间
	 */

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("CategoryTop10Application").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//TODO 0、获取原始数据，并转换为样例类
		val lineDataRDD: RDD[String] = sc.textFile("input/user_visit_action.csv")
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

		//TODO 1、 将原始数据进行结构变换：(session, (page_id, action_time))
		val datasRDD: RDD[(String, (Long, String))] = actionRDD.map(action => {
			(action.session_id, (action.page_id, action.action_time))
		})

		//TODO 2、 将转换结构后的数据根据session进行分组，(session, Iterator[(page_id, actiong_time)])
		val groupRDD: RDD[(String, Iterable[(Long, String)])] = datasRDD.groupByKey()

		//TODO 3、 对分组后的数据进行升序排列
		val durationRDD: RDD[(String, List[(Long, Long)])] = groupRDD.mapValues(datas => {
			val sortList: List[(Long, String)] = datas.toList.sortWith {
				(left, right) => left._2 < right._2
			}
			//TODO 4、 排序候得数据进行拉链处理:((page_id1, action_time1),(page_id2, action_time2)....)
			val zipList: List[((Long, String), (Long, String))] = sortList.zip(sortList.tail)

			//TODO 5、 将数据转换为指定结构:(page_id1, (action_time2 - action_time1))
			zipList.map {
				case (action1, action2) => {
					val time1: Long = DateUtil.parseStringToTimestap(action1._2)
					val time2: Long = DateUtil.parseStringToTimestap(action2._2)
					//转换结构为(page_id, (action_time2 - action_time1))
					(action1._1, time2 - time1)

				}
			}
		})

		//转换结构得(page_id, duration)
		val flatMapRDD: RDD[(Long, Long)] = durationRDD.map(_._2).flatMap(list=>list)

		//TODO 6、 转换后的数据根据page_id进行分组:(page_is, Iterator[action_time...])
		 val resultRDD: RDD[(Long, Iterable[Long])] = flatMapRDD.groupByKey()

		//TODO 7、 计算每个页面平均停留时间：timeList.sum / timeList.size
		resultRDD.foreach{
			case (pageid, timeList) =>{
				println("页面" + pageid + "\t平均停留时间为： " + (timeList.sum / timeList.size))
			}
		}

		sc.stop()
	}

}
