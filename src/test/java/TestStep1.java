import com.yxl.itemcf.s1.S1_Mapper;
import com.yxl.itemcf.s1.S1_Reducer;
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
 * 针对Step1 的单元测试
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午11:40
 */
public class TestStep1 {

    private MapDriver<LongWritable,Text,IntWritable,Text> mapDriver;
    private ReduceDriver<IntWritable,Text,IntWritable,Text> reduceDriver;
    private MapReduceDriver<LongWritable,Text,IntWritable,Text,IntWritable,Text> mapReduceDriver;

    @Before
    public void setUp(){
        S1_Mapper mapper = new S1_Mapper();
        S1_Reducer reducer = new S1_Reducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException{
        mapDriver.withInput(new LongWritable(),new Text("1,101,5.0"));
        mapDriver.withOutput(new IntWritable(1), new Text("101:5.0"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<Text> values = Arrays.asList(new Text("101:5.0"),new Text("103:2.5"),new Text("102:3.0"));
        reduceDriver.withInput(new IntWritable(1),values);
        reduceDriver.withOutput(new IntWritable(1), new Text("101:5.0,103:2.5,102:3.0"));   //由于是单机单元测试,因此需要顺序
        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
        mapReduceDriver.withInput(new LongWritable(),new Text("1,101,5.0"));
        mapReduceDriver.withInput(new LongWritable(),new Text("1,103,2.5"));
        mapReduceDriver.withInput(new LongWritable(),new Text("1,102,3.0"));
        mapReduceDriver.withOutput(new IntWritable(1), new Text("101:5.0,103:2.5,102:3.0"));
        mapReduceDriver.runTest();

    }


}
