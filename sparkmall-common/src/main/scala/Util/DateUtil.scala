package Util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

		/**
		 * 将指定时间字符串按照特定格式解析为日期对象
		 *
		 * @param str    时间
		 * @param f 格式
		 * @return 时间对象
		 */
		def parseStringToDate(str: String, f: String = "yyyy-MM-dd HH:mm:ss"): Date = {
			val format = new SimpleDateFormat(f)
			format.parse(str)
		}

		def parseStringToTimestap(str: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
			parseStringToDate(str, format).getTime
		}


}
