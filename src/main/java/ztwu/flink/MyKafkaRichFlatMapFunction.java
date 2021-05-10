package ztwu.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.ParameterizedType;

public class MyKafkaRichFlatMapFunction<OUT> extends RichFlatMapFunction<String, OUT> {
	private static Logger logger = LogManager.getLogger(MyKafkaRichFlatMapFunction.class);
	@Override
	public void flatMap(String value, Collector<OUT> collector) {
		try {
			if(value != null && !"".equals(value)){
				Class<OUT> tClass = (Class<OUT>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
				OUT out = JSONObject.parseObject(value, tClass);
				collector.collect(out);
			}
		} catch (Exception e) {
			logger.error("AbstractKafkaRichFlatMapFunction 发生异常：" + value, e);
		}
	}
}