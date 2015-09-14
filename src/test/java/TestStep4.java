import com.yxl.itemcf.s1.S1_Mapper;
import com.yxl.itemcf.s1.S1_Reducer;
import com.yxl.itemcf.s4.S4_Mapper;
import com.yxl.itemcf.s4.S4_Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 针对Step4 的单元测试, 正常来说 S4 应该拆分成2个 MR 程序,然后作为多个输出 传入 s5 ，这样就能分别做MRUnit了
 * 但为了 着重于 itemcf 而不是MRUnit 所以这里合一了
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午11:40
 */
public class TestStep4 {

//    private MapDriver<LongWritable, Text, Text, Text> mapDriver; //Mapper 无法测试 因为 InputSpilt 是 Mock的
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
//    private MapReduceDriver<LongWritable,Text,Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp(){
//        S4_Mapper mapper = new S4_Mapper();
        S4_Reducer reducer = new S4_Reducer();

//        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
//        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testReducer() throws IOException{
        List<Text> values = Arrays.asList(new Text("A:103,4"),//new Text("A:105,2"),new Text("A:101,5"),new Text("A:106,2"),new Text("A:107,1"),new Text("A:104,4"),new Text("A:102,3"),
                new Text("B:1,5.0")//,new Text("B:5,4.0"),new Text("B:4,5.0"),new Text("B:3,2.0"),new Text("B:2,2.0")
        );
        reduceDriver.withInput(new Text("101"),values);
        reduceDriver.withOutput(new Text("1"), new Text("103,20.0"));   //由于是单机单元测试,因此需要顺序
        reduceDriver.runTest();
    }

}
