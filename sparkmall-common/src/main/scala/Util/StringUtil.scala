package Util

object StringUtil {

	/**
	 * 判断字符串不为空
	 * @param s
	 * @return true
	 */
	def isNotEmpty(s: String): Boolean = {
		s != null && !"".equals(s.trim)
	}

}
