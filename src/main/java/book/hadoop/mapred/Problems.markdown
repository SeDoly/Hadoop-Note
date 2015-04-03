#GroupingComparator
    实现RawComparator，方法`public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)`若简单的将b1,b2通过新建
    String的方式进行比较，会出现错误(莫名奇妙多了空格或者回车符)。而通过DataInputBuffer对象(这里定为buffer)，执行buffer.reset(b1,s1,l1)
    后，通过Text对象的readFields(buffer)读取没问题，不知道为什么。可能与编解码有关？这里不知道在Reducer端做grouping时如何将原来的key
    (这里为Text对象)转换为上述数组对象的。