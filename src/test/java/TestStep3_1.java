import com.yxl.itemcf.s2.S2_Mapper;
import com.yxl.itemcf.s2.S2_Reducer;
import com.yxl.itemcf.s3_1.S3_1_Mapper;
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
 * author: xiaolong.yuanxl
 * date: 2015-09-14 上午9:09
 */
public class TestStep3_1 {

    private MapDriver<LongWritable,Text,IntWritable,Text> mapDriver;

    @Before
    public void setUp(){
        S3_1_Mapper mapper = new S3_1_Mapper();

        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2\t101:2.0,102:2.5"));
        mapDriver.withOutput(new IntWritable(101),new Text("2:2.0"));
        mapDriver.withOutput(new IntWritable(102),new Text("2:2.5"));
        mapDriver.runTest();
    }

}
