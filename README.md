# parquet-plus
一个增强parquet文件方便读写的工具类

[![author](https://img.shields.io/badge/author-HWY-red.svg)](https://github.com/HWYWL)

通过反射自动生成MessageType，自动拼装数据结构，遵循简单的原则，你给我数据我给你parquet文件。
目前支持的数据类型如下：
- int、Integer
- long、Long
- float、Float
- double、Double
- String

### 安装
**maven**
```
<dependency>
    <groupId>com.github.hwywl</groupId>
    <artifactId>parquet-plus</artifactId>
    <version>1.0.3-RELEASE</version>
</dependency>
```

**Gradle**
```
implementation 'com.github.hwywl:parquet-plus:1.0.3-RELEASE'
```

### 使用

可以查看github中的源码，不看也行，很简单的，使用如下：
```java
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
     * 将parquet文件转为对象集合,并只拿2条数据
     *
     * @throws IOException
     */
    @Test
    public void ParquetReadBeanMaxLineTest() throws IOException {
        List<TestModel> models = ParquetUtil.readParquetBean(filePath, 2, TestModel.class);
        System.out.println(models);
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
            arrayList.add(new TestModel(2, 3, 6L, "校花", 10));
        }

        return arrayList;
    }
}
```

### 1.0.3-RELEASE 版本更新
1. 开放底层api

### 1.0.2-RELEASE 版本更新
1. 修复bug
2. 增加更简便的API

### 1.0.1-RELEASE 版本更新
1. 增加生成parquet文件
2. 增加parquet文件读取