import com.yxl.itemcf.s3_1.S3_1_Mapper;
import com.yxl.itemcf.s3_2.S3_2_Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * author: xiaolong.yuanxl
 * date: 2015-09-14 上午9:09
 */
public class TestStep3_2 {

    private MapDriver<LongWritable,Text,Text,IntWritable> mapDriver;

    @Before
    public void setUp(){
        S3_2_Mapper mapper = new S3_2_Mapper();

        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("101:101\t5"));
        mapDriver.withOutput(new Text("101:101"), new IntWritable(5));
        mapDriver.runTest();
    }

}
