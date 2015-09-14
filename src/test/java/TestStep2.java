import com.yxl.itemcf.s1.S1_Mapper;
import com.yxl.itemcf.s1.S1_Reducer;
import com.yxl.itemcf.s2.S2_Mapper;
import com.yxl.itemcf.s2.S2_Reducer;
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
 * 针对Step2 的单元测试
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-11 下午5:46
 */
public class TestStep2 {

    private MapDriver<LongWritable,Text,Text,IntWritable> mapDriver;
    private ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable,Text,Text,IntWritable,Text,IntWritable> mapReduceDriver;


    @Before
    public void setUp(){
        S2_Mapper mapper = new S2_Mapper();
        S2_Reducer reducer = new S2_Reducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2\t101:2.0,102:2.5"));
        mapDriver.withOutput(new Text("101:101"), new IntWritable(1));
        mapDriver.withOutput(new Text("101:102"), new IntWritable(1));
        mapDriver.withOutput(new Text("102:101"),new IntWritable(1));
        mapDriver.withOutput(new Text("102:102"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1));
        reduceDriver.withInput(new Text("101:101"),values);
        reduceDriver.withOutput(new Text("101:101"),new IntWritable(3));   //由于是单机单元测试,因此需要顺序
        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
        mapReduceDriver.withInput(new LongWritable(),new Text("2\t101:2.0,102:2.5"));
        mapReduceDriver.withInput(new LongWritable(),new Text("2\t101:2.0,102:2.5"));
        mapReduceDriver.withOutput(new Text("101:101"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("101:102"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("102:101"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("102:102"), new IntWritable(2));
        mapReduceDriver.runTest();

    }


}
