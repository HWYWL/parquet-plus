import com.github.hwywl.exception.CustomException;
import com.github.hwywl.utils.ParquetUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author YI
 * @description
 * @Test
 * @date create in 2021/9/2 18:38
 */
public class ParquetTest {
    String filePath = "F:\\a.parquet";

    /**
     * 写入一个数据到parquet文件
     *
     * @throws IOException
     * @throws CustomException
     */
    @Test
    public void ParquetWriteTest() throws IOException, CustomException {
        TestModel model = getModel();
        ParquetUtil.writerParquet(filePath, model);
    }

    /**
     * 写入多个数据到parquet文件
     *
     * @throws IOException
     * @throws CustomException
     */
    @Test
    public void ParquetWriteListTest() throws IOException, CustomException {
        List<TestModel> models = getModels();
        ParquetUtil.writerParquet(filePath, models);
    }

    /**
     * 将parquet文件转为对象集合
     *
     * @throws IOException
     */
    @Test
    public void ParquetReadBeanTest() throws IOException {
        List<TestModel> models = ParquetUtil.readParquetBean(filePath, TestModel.class);
        System.out.println(models);
    }

    /**
     * 将parquet文件转为Map集合
     *
     * @throws IOException
     */
    @Test
    public void ParquetReadMapTest() throws IOException {
        List<Map<String, Object>> maps = ParquetUtil.readParquetMap(filePath);
        System.out.println(maps);
    }

    /**
     * 测试数据
     *
     * @return
     */
    private TestModel getModel() {
        return new TestModel(2, 3, 6L, "校花", 10);
    }

    /**
     * 测试数据
     *
     * @return
     */
    private List<TestModel> getModels() {
        List<TestModel> arrayList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            TestModel testModel = new TestModel(2, 3, 6L, "校花", 10);
        }

        return arrayList;
    }
}
