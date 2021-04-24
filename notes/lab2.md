# Lab 2

这个实验需要完成：增删查改，页面置换算法。

## Exercise 1

实现 Filter 和 Join 操作，文档中提到已经提供了 Project 和 OrderBy 的实现。用 IDEA 查看 Operator 的实现类，我们可以发现有 8 个实现，这些实现类对应着一个具体的操作：聚合，删除，过滤，散列版本的连接，插入，连接，排序，投影。Operator 实现了 OpIterator 的部分方法，

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225114910667-730918637.png)

- Predicate 和 JoinPredicate 直接按要求写即可，测试直接过
- 实现 Filter 之前，先看一看 Operator 和 Project。为了实现 Filter，我们需要知道数据从哪里来，又要去到哪儿。数据来源是从一个 OpIterator 中拿，如果是一元操作符，那么需要一个 OpIterator。数据的去向，是 fetchNext 中返回的 Tuple。在 fetchNext 中，需要执行具体的操作。比如，Project 中的 fetchNext，它就筛选了属性。Filter 中的 fetchNext 需要筛选元组。写完一次过测试。值得一提的是，open 和 close，这两个和构造析构道理类似。先进行父类的构造，然后再进行子类的。先进行子类的析构，然后再执行父类的。
- 接下来实现 Join，这是一个二元操作符，那么需要有两个 OpIterator。对于 Join，最普通的方法就是进行两遍扫描，然后返回匹配的元组，并且注释中提到，我们需要将两个元组合并到一起。那么 Join 有没有办法优化呢？
- Database System Concepts 这本书有个网站上有配套的 PPT，可以看第 15 章的 PPT，就有 Join 的实现方法。PPT 中列举了 5 种方法，嵌套循环，分块嵌套循环（因为内存放不下），建索引的嵌套循环，排序然后再连接，基于散列的连接。要搞懂还需要点时间，还是先做一个嵌套循环熟悉一下这个 SimpleDB 中能做什么，不能做什么先吧。
- 虽然思路是最简单的嵌套循环，但是测试一跑，只有直接获取属性的那个通过了，干啊。报了两个空指针异常。仔细查一查，错误出现在合并 Tuple 的操作中。之前设计 Tuple 的时候，初始化的每个属性都是空，我在合并 Tuple 的时候，直接用的 addAll，这意味着并没有清楚前面的空值，所以后面就出现了空指针异常。ok，这波之后，rewind 通过了。
- 还有两个测试没有通过啊。最后经过一番波折，在 fetchNext 那里找到了问题，因为没有重置 child2 的指针！在适当的时机重置后，两个测试都通过了。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225172645519-163953867.png)

## Exercise 1 补充: Join 的实现方法

虽然通过了测试用例，但是不应该满足于此，应该尝试优化一下 Join。还有把 Join 的几种实现方法都学一学。看完 PPT 后，除了嵌套循环，没有一个我能做。🙃

##### Nested-Loop Join

嵌套循环，判断是否满足条件，如果满足条件就加入到结果集。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225180123413-1789904465.png)

#### Block Nested-Loop Join

这个 PPT 里面提到了一个情况，那就是如果表太多了，无法一次性全部读进来的情况下，需要将表分块读进来。如果分块读取，还继续使用第一个算法的话，性能会特别糟糕。假设 n 表示表的长度，b 表示分块数目。如果连接表 r 和表 s，那么需要读取次数为：$n_r \times b_s + b_r $。因为表是很长的，所以 $n_r$ 肯定不会小，那么读取磁盘的次数就大大增加了。因此，为了改进这个缺点，有如下的算法，两两分块之间进行一次嵌套循环。总的时间复杂度是一样的，但是 IO 操作大大减小了。这启发我们以后考虑大规模的数据的时候，不仅仅要考虑理论的复杂度，还要考虑实际运行时候的硬件限定，比如内存有限，需要进行 IO。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225181415601-1277728830.png)

#### Indexed Nested-Loop Join

嵌套循环中第二个循环，本质上是在寻找满足条件的元组，那么可以在第二个表上建立索引，第二个循环就可以替换成索引了。PPT 中提到存在一个约束，要是等值连接或者自然连接才可以用索引替代扫描。假设操作用的是等值连接，在 SimpleDB 中，我们可以用需要判断的域来建立散列表，用 Objects.hash 来获取散列值。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225182104511-1832586921.png)

#### Merge-Join

这个思路比较有意思，首先给两个表排序，然后用双指针来遍历。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225183031821-1177004561.png)

#### Hash-Join

使用两次散列，第一次散列用来分块，第二次散列用来匹配。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225184601408-94055277.png)

## Exercise 2

实现聚合操作（count，sum，avg，max，min），聚合操作中有一个 group 分组要实现。只要在一个属性上实现聚合操作。

Aggregate 这个类和之前的 Join 一样，都是 Operator 的具体实现。Aggregate 中将不同类型的聚合操作抽离出来，比如对于整数型的聚合操作，需要专门写一个整数型的 Aggregator 来处理。Aggregate 中，每次得到数据源 child 的时候，我们就需要初始化一次 Aggregator 来获得聚合操作后的数据。

- 首先，我先把 Aggregate 的内容填写了一下，然后开始看每个具体的 Aggregator。Aggregator 内部需要维护分组数据，维护 (groupValue, aggregateValue)，这个好做，用散列表来做就好了。先实现 StringAggregator，按部就班，写好之后测试就可以通过了。顺便一提，散列表一开始用 Integer 映射到 Integer，但是后面需要返回一个 OpIterator 的时候，里面的 next 方法返回的 Tuple 需要设置 AggregateValue，因此将散列表的 key 改成了 Field 类型。
- 接下来实现 IntegerAggregator。实现 count，sum，max，min 都只需要维护一个值就好了，但是实现 avg 还需要维护多一个数。因此，建立散列表的时候，value 的类型需要存储两个整数。写完，测试仨下就通过了，因为 isClosed 没有设置初始值 failed 两次，布尔值默认是 false。
- 接下来就是 Aggregate 了。把 Aggregate 填好空之后，跑一下测试用例，有一个没有通过，sumStringGroupBy。
- 之后面向测试用例编程，单步调试看看错误在哪里，发现是返回的 Tuple 中 groupValue 的类型不一致，因为没有用上构造器传进来的 gFieldType，用上了就好了。在跑一下，AggregateTest 就通过了。试试看 systemtest 吧。
- systemtest 失败了一个呢，testAverageNoGroup。是一个越界的问题，访问了 list 中 -1 位置的 元素，这个 -1 很有可能来自于 gField，应该是前面有些地方没有判断导致的。仔细跟踪到 mergeTupleIntoGroup，StringAggregator 中已经处理好了，但是 IntegerAggregator 中没有做同样的处理。疏忽大意。改好之后，Exercise 2 也就顺利 pass 了。

![](https://img2020.cnblogs.com/blog/1616773/202102/1616773-20210225233149278-1844058119.png)

## Exercise 3

实现在内存中插入删除数据。

- 首先完成的 HeapPage.java 上面的插入和删除。对于插入，需要找到空位并且标记 bitmap，删除只需要标记 bitmap 就好。写好之后，一测发现只过了一个，原来是很多异常的情况没有处理好。注意看 insertTuple 和 deleteTuple 中的注释，需要抛出异常。设置好条件，抛出异常，之后测试(HeapPageWriteTest)就顺利通过了。
- 接下来写 HeapFile.java。HeapFile 中的插入元组，删除元组方法，统一使用 BufferPool 中缓存的 Page，如果那个 Page 不在内存中，那么由 BufferPool 去读取。文档中有那么一句话："Note that it is important that the HeapFile.insertTuple() and HeapFile.deleteTuple() methods access pages using the BufferPool.getPage() method"，写好了 HeapFile.java 中的 insertTuple 之后，测试没有通过。仔细看了一遍测试代码，测试中向一个空的 HeapFile 写入元组。这个元组是通过 Utility.java 中的 getHeapTuple 获取的，这个方法没有创建一个表，所以执行到后面的时候，报错了，说找不到这个表。
- 一开始觉得是测试的错误，认为他没有设置好 RecordId。仔细看文档，才会发现，需要我们自己去设置 RecordId。这个 RecordId 依赖于一个 PageId，PageId 需要去找到文件上可以填入元组的空闲页，如果没有，需要创建新的页。对于创建的新页面，需要写入到磁盘，然后通过 BufferPool 读取进来。
- 之后还需要填写 BufferPool 中 insert 和 delete 的部分，这个调用 PageFile 中相应的部分即可。第一次测试没有通过，原因在于 Tuple 中的 equals 方法没有正确实现。这个 equals，需要元组的 schema 和属性值相等即可。写好 equals，测试就通过了。

![](https://img2020.cnblogs.com/blog/1616773/202104/1616773-20210424182737857-2129065331.png)

## Exercise 4

- Insert 和 Delete 随便写了一写，Insert 的单元测试可以通过了，但是系统测试还不行。主要问题是在 DeleteTest 中没有通过。
- 最后定位到问题，在 HeapPage 的迭代器中。DeleteTest 中一共创建了 1000+ 个元组，但是实际上只删除了 550 个。经过排查，发现从最底层的 Operator 返回的元组，只有 550 个。因此，认为问题应该出现在比较底层的位置。最终找到了 HeapPage 的 Iterator，发现 hasNext 的判断方法是 `i < (numSlots - getNumEmptySlots())`，这么判断是存在问题的。因为删除的元组，可能是迭代器前面的元组，接着使用 `getNumEmptySlots()` 得到的数量减少了，实际上不应该减少。
- 修改了 HeapPage 中 Iterator 的逻辑之后，重新测试，原来失败的判断，现在成功了。但是，后续的断言仍然有问题。出现问题的地方是，删除了元组之后，使用 SeqScan 仍然扫描到了元组。我很快就发现了问题所在的地方，在 HeapFile 中的 hasNext 判断。如果元组横跨了几个页，那么应该跳到那个页。不应该像代码中写的那样，获取下一页，然后返回 true。这么做是错误的，因为下一页可能什么都没有。
- 写好之后，逻辑稍微有点问题，经过调整之后，测试通过了。但是修改了 HeapFile 这部分的逻辑之后，前面的一个测试失败了。修改了 BufferPool 中的 insertTuple 之后，才能通过测试。markDirty 之后将内容写入到磁盘。感觉这样实在是有点奇怪。不知道应该如何处理，暂且为了通过测试，那么写吧。

![](https://img2020.cnblogs.com/blog/1616773/202104/1616773-20210424171135526-2059394794.png)

## Exercise 5

在实现前面的基础上，不做修改即可通过测试。之前的策略是先进先出。

![](https://img2020.cnblogs.com/blog/1616773/202104/1616773-20210424212814086-971529072.png)


## 总结

所有的测试都通过了。距离 Lab1 已经相隔了两个月，这星期重新捡起来的时候，还把以前写的代码看了看。整体难度不算特别大，只要耐心一点，仔细调试下去，总是可以把测试通过的。

这一个实验的有五个练习。练习一和二，实现各种操作符。练习三和四，完善文件、缓存相关的操作，最后实现插入和删除。练习五，实现页面置换算法，这里就选择了最简单的先入先出。

![](https://img2020.cnblogs.com/blog/1616773/202104/1616773-20210424214420592-1220068059.png)

