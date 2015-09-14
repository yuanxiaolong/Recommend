import com.yxl.itemcf.s1.S1_Mapper;
import com.yxl.itemcf.s1.S1_Reducer;
import com.yxl.itemcf.s5.S5_Mapper;
import com.yxl.itemcf.s5.S5_Reducer;
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
 * 针对Step5 的单元测试
 *
 * author: xiaolong.yuanxl
 * date: 2015-09-10 下午11:40
 */
public class TestStep5 {

    private MapDriver<LongWritable,Text,Text,Text> mapDriver;
    private ReduceDriver<Text,Text,Text,Text> reduceDriver;
    private MapReduceDriver<LongWritable,Text,Text,Text,Text,Text> mapReduceDriver;

    @Before
    public void setUp(){
        S5_Mapper mapper = new S5_Mapper();
        S5_Reducer reducer = new S5_Reducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException{
        mapDriver.withInput(new LongWritable(),new Text("3\t107,2.0"));
        mapDriver.withOutput(new Text("3"), new Text("107,2.0"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<Text> values = Arrays.asList(new Text("107,2.0"),new Text("107,4.0"),new Text("107,4.5"));
        reduceDriver.withInput(new Text("3"),values);
        reduceDriver.withOutput(new Text("3"), new Text("107,10.5"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException{
        mapReduceDriver.withInput(new LongWritable(),new Text("3\t107,2.0"));
        mapReduceDriver.withInput(new LongWritable(),new Text("3\t107,4.0"));
        mapReduceDriver.withInput(new LongWritable(),new Text("3\t107,4.5"));
        mapReduceDriver.withOutput(new Text("3"), new Text("107,10.5"));
        mapReduceDriver.runTest();
    }


}
