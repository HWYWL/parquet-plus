package com.github.hwywl.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import com.github.hwywl.exception.CustomException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parquet转换工具类
 *
 * @author YI
 * @date 2021-9-3 18:30:11
 */
public class ParquetUtil {
    /**
     * 将数据写入parquet文件
     *
     * @param parquetPath parquet路径
     * @param bean        对象
     */
    public static <T> void writerParquet(String parquetPath, T bean) throws IOException, CustomException {
        Group group = getGroup(bean);
        ParquetWriter<Group> writer = getParquetWriter(parquetPath, bean.getClass());

        writer.write(group);
        writer.close();
    }

    /**
     * 将集合数据写入parquet文件
     *
     * @param parquetPath parquet路径
     * @param beans       数据集合
     */
    public static <T> void writerParquet(String parquetPath, List<T> beans) throws IOException, CustomException {
        if (CollUtil.isEmpty(beans)) {
            return;
        }

        ParquetWriter<Group> writer = getParquetWriter(parquetPath, beans.get(0).getClass());

        for (T bean : beans) {
            Group group = getGroup(bean);
            writer.write(group);
        }

        writer.close();
    }

    /**
     * 读取parquet文件信息，转为bean
     *
     * @param parquetPath 文件路径
     * @param clazz       对象字节码
     * @return 已格式化的bean
     * @throws IOException
     */
    public static <T> List<T> readParquetBean(String parquetPath, Class<T> clazz) throws IOException {
        return readParquetBean(parquetPath, -1, clazz);
    }

    /**
     * 读取parquet文件信息，转为bean
     *
     * @param parquetPath 文件路径
     * @param maxLine     读取的行数
     * @param clazz       对象字节码
     * @return 已格式化的bean
     * @throws IOException
     */
    public static <T> List<T> readParquetBean(String parquetPath, int maxLine, Class<T> clazz) throws IOException {
        List<T> dataList = new ArrayList<>();

        Path path = new Path(parquetPath);
        ParquetReader<Object> reader = ParquetReader.builder(new AvroReadSupport<>(), path).build();

        Object line;
        while ((line = reader.read()) != null && maxLine != 0) {
            dataList.add(JSONUtil.parseObj(line.toString()).toBean(clazz));
            maxLine--;
        }

        return dataList;
    }

    /**
     * 读取parquet文件信息，转为map
     *
     * @param parquetPath 文件路径
     * @return 已格式化的bean
     * @throws IOException
     */
    public static List<Map<String, Object>> readParquetMap(String parquetPath) throws IOException {
        return readParquetMap(parquetPath, -1);
    }

    /**
     * 读取parquet文件信息，转为map
     *
     * @param parquetPath 文件路径
     * @param maxLine     读取的行数
     * @return 已格式化的bean
     * @throws IOException
     */
    public static List<Map<String, Object>> readParquetMap(String parquetPath, int maxLine) throws IOException {
        List<Map<String, Object>> dataList = new ArrayList<>();

        Path path = new Path(parquetPath);
        ParquetReader<Object> reader = ParquetReader.builder(new AvroReadSupport<>(), path).build();

        Object line;
        while ((line = reader.read()) != null && maxLine != 0) {
            dataList.add(BeanUtil.beanToMap(JSONUtil.parseObj(line.toString())));
            maxLine--;
        }

        return dataList;
    }

    /**
     * 获取生成parquet文件的 ParquetWriter
     *
     * @param parquetPath 生成parquet的路径
     * @param clazz       对象字节码
     * @return ParquetWriter
     * @throws IOException
     */
    public static <T> ParquetWriter<Group> getParquetWriter(String parquetPath, Class<T> clazz) throws IOException, CustomException {
        MessageType messageType = getInstance(clazz);
        Path path = new Path(parquetPath);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType, configuration);
        return new ParquetWriter<>(path, configuration, new GroupWriteSupport());
    }

    /**
     * 自动构建Group
     *
     * @param bean 数据
     * @return
     */
    public static <T> Group getGroup(T bean) throws CustomException {
        Class<?> clazz = bean.getClass();
        GroupFactory factory = new SimpleGroupFactory(getInstance(clazz));
        Group group = factory.newGroup();
        Map<String, Object> map = BeanUtil.beanToMap(bean);

        // 生成group
        map.forEach((key, value) -> {
            PropertyDescriptor descriptor = BeanUtil.getPropertyDescriptor(clazz, key);
            Class<?> propertyType = descriptor.getPropertyType();
            if (int.class == propertyType || Integer.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.toDouble(value), 0).intValue());
            } else if (long.class == propertyType || Long.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.toDouble(value), 0).longValue());
            } else if (float.class == propertyType || Float.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.toDouble(value), 0).floatValue());
            } else if (double.class == propertyType || Double.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.toDouble(value), 0).doubleValue());
            } else if (String.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.toStr(value), ""));
            } else if (boolean.class == propertyType || Boolean.class == propertyType) {
                group.add(key, ObjectUtil.defaultIfNull(Convert.convert(Boolean.class, value), false));
            }
        });

        return group;
    }

    /**
     * 自动构建生成parquet的schema
     *
     * @param clazz 对象
     * @return MessageType
     * @throws CustomException
     */
    private static MessageType getInstance(Class<?> clazz) throws CustomException {
        Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
        PropertyDescriptor[] propertyDescriptors = BeanUtil.getPropertyDescriptors(clazz);

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            String fieldName = propertyDescriptor.getName();
            Class<?> propertyType = propertyDescriptor.getPropertyType();
            if (int.class == propertyType || Integer.class == propertyType) {
                messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(fieldName);
            } else if (long.class == propertyType || Long.class == propertyType) {
                messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
            } else if (float.class == propertyType || Float.class == propertyType) {
                messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(fieldName);
            } else if (double.class == propertyType || Double.class == propertyType) {
                messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(fieldName);
            } else if (String.class == propertyType) {
                messageTypeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).named(fieldName);
            } else if (Map.class == propertyType || Map.class.isAssignableFrom(propertyType)) {
                throw new CustomException("类型暂不支持:：" + propertyType);
            } else if (List.class == propertyType || List.class.isAssignableFrom(propertyType)) {
                throw new CustomException("类型暂不支持:：" + propertyType);
            } else {
                // 不支持的类型自动抛出异常
                throw new CustomException("类型不支持:：" + propertyType);
            }
        }

        return messageTypeBuilder.named("Pair");
    }
}