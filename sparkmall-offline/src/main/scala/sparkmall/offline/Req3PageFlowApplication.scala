package sparkmall.offline

import module.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Req3PageFlowApplication {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("CategoryTop10Application").setMaster("local[*]")
		val sc = new SparkContext(conf)

		//TODO 1、获取原始数据，并转换为样例类
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
		//同一个RDD使用了两次，可以ccache一下
		actionRDD.cache()

		//先计算分母：分母为单个页面被访问的数量
		//TODO 1.1、对原始数据过滤，保留需要进行统计的字段pageId
		//保留1，2，3，4，5，6，7页面
		val pageIds: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
		//形成拉链页面，1-2，2-3
		val zipPageIds: List[String] = pageIds.zip(pageIds.tail).map {
			case (pageId1, pageId2) => {
				pageId1 + "-" + pageId2
			}
		}

		val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
			pageIds.contains(action.page_id)
		})

		//TODO 1.2、过滤后的数据转换转换结构为：(pageId， 1)
		val pageIdTo1RDD: RDD[(Long, Long)] = filterRDD.map {
			action => (action.page_id, 1L)
		}

		//TODO 1.3、转换结构后的数据聚合，由 (pageId， 1) ==> (pageId, sum)
		val pageIdReduceRDD: RDD[(Long, Long)] = pageIdTo1RDD.reduceByKey(_ + _)
		val pagesToSum: Map[Long, Long] = pageIdReduceRDD.collect.toMap

		//分子：页面间跳转的次数
		//TODO 2.1、将原始数据根据session进行分组：(session, Iterator[UserVisitAction])
		val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

		//TODO 2.3、分组后的数据按照时间升序排列，由(session, Iterator[UserVisitAction]) ==> (session, List[pageid1, pageid2])
		val sessionToZip: RDD[(String, List[(String, Long)])] = sessionGroupRDD.mapValues {
			datas => {
				val actions: List[UserVisitAction] = datas.toList.sortWith {
					(left, right) => {
						left.action_time < right.action_time
					}
				}
				val ids: List[Long] = actions.map(_.page_id)

				//TODO 2.3、将集合中的pageId形成拉链效果(1-2, 1)、(2-3,1)
				val zipList: List[(Long, Long)] = ids.zip(ids.tail)
				zipList.map {
					case (pageId1, pageId2) => {
						(pageId1 + "-" + pageId2, 1L)
					}
				}
			}
		}
		val zipRDD: RDD[(String, Long)] = sessionToZip.map(_._2).flatMap(list => list)

		//TODO 2.4、过滤无效数据，如：1-9(超出统计范围)
		val zipFilterRDD: RDD[(String, Long)] = zipRDD.filter {
			case (pageFlow, one) => {
				zipPageIds.contains(pageFlow)
			}
		}

		//TODO 2.5、将拉链后的数据聚合分析(pageid1-pageid2, sum)
		val pageReduceRDD: RDD[(String, Long)] = zipFilterRDD.reduceByKey(_ + _)

		//TODO 3、计算单跳转换率
		//分子数据/分母数据 => (pageid1-pageid2).sum / pageid1.sum
		pageReduceRDD.foreach {
			case (pageFlow, sum) => {
				val pageId: String = pageFlow.split("-")(0)
				println(pageFlow + " = " + sum.toDouble / pagesToSum.getOrElse(pageId.toLong, 1L))
			}
		}

	}

}
