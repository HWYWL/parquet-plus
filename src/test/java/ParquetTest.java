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

    @Test
    public void ParquetWriteTest() throws IOException, CustomException {
        TestModel model = getModel();
        ParquetUtil.writerParquet(filePath, model);
    }

    @Test
    public void ParquetWriteListTest() throws IOException, CustomException {
        List<TestModel> models = getModels();
        ParquetUtil.writerParquet(filePath, models);
    }

    @Test
    public void ParquetReadBeanTest() throws IOException {
        List<TestModel> models = ParquetUtil.readParquetBean(filePath, TestModel.class);
        System.out.println(models);
    }

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
        TestModel testModel = new TestModel();
        testModel.setApp_id(2);
        testModel.setMoney(2.03);
        testModel.setSvip_level(1);
        testModel.setSvip_remain(1L);
        testModel.setName("校花");

        return testModel;
    }

    /**
     * 测试数据
     *
     * @return
     */
    private List<TestModel> getModels() {
        List<TestModel> arrayList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            TestModel testModel = new TestModel();
            testModel.setApp_id(2);
            testModel.setMoney(2.03);
            testModel.setSvip_level(1);
            testModel.setSvip_remain(1L);
            testModel.setName("校花");
            arrayList.add(testModel);
        }

        return arrayList;
    }
}
