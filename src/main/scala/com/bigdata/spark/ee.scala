package com.bigdata.spark

class ee {

  /**
    * 1.APP层接收controller，然后执行excutor方法
    *
    * 2.Controller接收service，然后执行data_analysis方法
    *
    * 3.service层接收Dao ,重写analysis方法，输出计算逻辑，返回给controller
    *
    * 4.Dao用于读取数据,并返回给data_analysis方法
    *
    * map 与 flatmap区别
    *  map函数后，RDD的值为 Array(Array("a","b"),Array("c","d"),Array("e","f"))

    *  flatMap函数处理后，RDD的值为 Array("a","b","c","d","e","f")

    *  即最终可以认为，flatMap会将其返回的数组全部拆散，然后合成到一个数组中
    *
    * reduceByKey():此方法会发生分组聚合，也就同意产生数据倾斜、可使用累加器代替
    *
    * 累加器的实现原理
    * 累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，
    * 在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。
    *
    * 广播变量
    * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。比如，如果你
    * 的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark
    * 会为每个任务分别发送。
    *
    * spark中计算数据时，使用的闭包操作，会导致变量被不同的Task所使用，那么会传递到多个executor多份，
    * 这样的话，executor内存中会存在大量数据冗余，性能会下降的比较厉害。
    * 第一个task去内存中拉取不到数据，内存就会去driver中拉取数据，放到内存中，然后task再去读取然后其他的task再去访问。
    *
    * Zip函数
    *
    * 　在Scala中存在好几个Zip相关的函数，比如zip，zipAll，zipped 以及zipWithIndex等等。我们在代码中也经常看到这样的函数，这篇文章主要介绍一下这些函数的区别以及使用。
        　　1、zip函数将传进来的两个参数中相应位置上的元素组成一个pair数组。如果其中一个参数元素比较长，那么多余的参数会被删掉。看下英文介绍吧：

        　　Returns a list formed from this list and another iterable collection by combining corresponding elements in pairs. If one of the two collections is longer than the other, its remaining elements are ignored.
        scala> val numbers = Seq(0, 1, 2, 3, 4)
        numbers: Seq[Int] = List(0, 1, 2, 3, 4)

        scala> val series = Seq(0, 1, 1, 2, 3)
        series: Seq[Int] = List(0, 1, 1, 2, 3)

        scala> numbers zip series
        res24: Seq[(Int, Int)] = List((0,0), (1,1), (2,1), (3,2), (4,3))
        　
        　　2、zipAll 函数和上面的zip函数类似，但是如果其中一个元素个数比较少，那么将用默认的元素填充。

        　　The zipAll method generates an iterable of pairs of corresponding elements from xs and ys, where the shorter sequence is extended to match the longer one by appending elements x or y
        /**
            * User: 过往记忆
            * Date: 14-12-17
            * Time: 上午10:16
            * bolg:
            * 本文地址：/archives/1225
            * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
            * 过往记忆博客微信公共帐号：iteblog_hadoop
            */
        scala> val xs = List(1, 2, 3)
        xs: List[Int] = List(1, 2, 3)

        scala> val ys = List('a', 'b')
        ys: List[Char] = List(a, b)

        scala> val zs = List("I", "II", "III", "IV")
        zs: List1 = List(I, II, III, IV)

        scala> val x = 0
        x: Int = 0

        scala> val y = '_'
        y: Char = _

        scala> val z = "_"
        z: java.lang.String = _

        scala> xs.zipAll(ys, x, y)
        res30: List[(Int, Char)] = List((1,a), (2,b), (3,_))

        scala> xs.zipAll(zs, x, z)
        res31: List[(Int, java.lang.String)] = List((1,I), (2,II), (3,III), (0,IV))
        　　3、zipped函数，这个不好翻译，自己看英文解释吧

        　　The zipped method on tuples generalizes several common operations to work on multiple lists.
        scala> val values = List.range(1, 5)
        values: List[Int] = List(1, 2, 3, 4)

        scala> (values, values).zipped toMap
        res34: scala.collection.immutable.Map[Int,Int] = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)

        scala> val sumOfSquares = (values, values).zipped map (_ * _) sum
        sumOfSquares: Int = 30
        　　4、zipWithIndex函数将元素和其所在的下表组成一个pair。

        　　The zipWithIndex method pairs every element of a list with the position where it appears in the list.
        scala> val series = Seq(0, 1, 1, 2, 3, 5, 8, 13)
        series: Seq[Int] = List(0, 1, 1, 2, 3, 5, 8, 13)

        scala> series.zipWithIndex
        res35: Seq[(Int, Int)] = List((0,0), (1,1), (1,2), (2,3), (3,4), (5,5), (8,6), (13,7))
        　　5、unzip函数可以将一个元组的列表转变成一个列表的元组

        　　The unzip method changes back a list of tuples to a tuple of lists.
        scala> val seriesIn = Seq(0, 1, 1, 2, 3, 5, 8, 13)
        seriesIn: Seq[Int] = List(0, 1, 1, 2, 3, 5, 8, 13)

        scala> val fibonacci = seriesIn.zipWithIndex
        fibonacci: Seq[(Int, Int)] = List((0,0), (1,1), (1,2), (2,3), (3,4), (5,5), (8,6), (13,7))

        scala> fibonacci.unzip
        res46: (Seq[Int], Seq[Int]) = (List(0, 1, 1, 2, 3, 5, 8, 13),List(0, 1, 2, 3, 4, 5, 6, 7))

        scala> val seriesOut = fibonacci.unzip _1
        seriesOut: Seq[Int] = List(0, 1, 1, 2, 3, 5, 8, 13)

        scala> val numbersOut = fibonacci.unzip _2
        numbersOut: Seq[Int] = List(0, 1, 2, 3, 4, 5, 6, 7)
    *
    *
    * 当idea出现 Sources not found for: 使用如下命令：
    * mvn dependency:resolve -Dclassifier=sources
    * */
}
