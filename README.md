# SparkProcessesOfflineData
spark处理随机生成的模拟离线数据。

## 一 、获取点击、下单和支付数量排名前 10 的品类    

```
	目标：点击（click）、下单（order）、支付（pay）品类数量Top10    
```

1. 读取日志数据    
2. 转换日志结构，给点击品类id加上类型标识“-click”，给这一次品类id加上计数表示“1”==>(click_category_id-click,1)
3. 转换结构后的数据分组聚合，由(click_category_id-click,1) ==> (click_category_id-click,sum) 
4. 聚合后的数据转换结构，由(click_category_id-click,sum) ==> (click_category_id,(click,sum)) 
5. 对转换格式后的聚合数据分组，转换为可迭代的对象，由(click_category_id,(click,sum)) ==> (click_category_id, Interator((click, sum)))
6. 分组的数据转化为样例类，把一条数据转化为一个对象
7. 转换格式后的数据降序排列取Top10

## 二、Top10 热门品类中 Top10 活跃 Session 统计   

```
	Session 即会话，是指在指定的时间段内在网站上发生的一系列互动。例如，一次会话可以包含多个网页或屏幕浏览、事件、社交互动和电子商务交易
  活跃即点击，sessionId：标识某个用户的一个访问session。热门品类Top10中，对每个品类获取该品类点击Top10的sessionId。
	10个种类，每个种类10个sessionId，一共100条数据   
```

1. 获取项目1中的ccotegory_id,对原始数据进行筛选过滤，筛选出Top10品类的数据   

2. 过滤后的数据转换结构，添加品类标识：click_category_id;添加session标识：sessionId;添加计数标识：1  ==>      (click_cotegory_id-sessionId, 1)   

3. 转换结构的数据进行聚合,由 (click_cotegory_id-sessionId, 1) ==> (click_cotegory_id-sessionId, sum)   

4. 聚合结果转换结构，由(click_cotegory_id-sessionId, sum) ==> (click_cotegory_id, (sessionId, sum))   

5. 转换结构后的数据根据品类id分组，形成可迭代对象      

   由 (click_cotegory_id, (sessionId, sum)) ==> (click_cotegory_id, Iterator[(sessionId, sum)])   

6. 分组后数据降序排列取Top10

## 三、页面单跳转化率统计   

```
	1-2, 2-3, 3-5 这样的页面跳转称为单跳。记，一个session访问页面3的次数是A，访问完3然后紧接着又访问页面5的次数为B
	3-5的单跳转化率 = B/A   分母为某个页面总的访问数，分子需要按访问时间排序，计算跳转页面的总访问数量
```

1. 获取原始数据，并抓换为样例类 
   分母：分母为单个页面被访问的数量 
2. 对原始数据过滤，保留需要进行统计的字段pageId（保留1，2，3，4，5，6，7页面）
3. 过滤后的数据转换转换结构为：(pageId， 1)
4. 转换结构后的数据聚合，由 (pageId， 1) ==> (pageId, sum)      
   分子：页面间跳转的次数 
5. 分组后的数据按照时间升序排列，由(session, Iterator[UserVisitAction]) ==> (session, List[pageid1, pageid2])
6. 将集合中的pageId形成拉链效果(1-2, 1)、(2-3,1)
7. 过滤无效数据，如：1-9(超出统计范围)
8. 将拉链后的数据统计分析(pageid1-pageid2, sum)
9. 计算单跳转换率
