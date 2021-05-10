package ztwu.flink;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class DateFormatFunction extends ScalarFunction {
	public String eval(Timestamp time, String format) {
		return new SimpleDateFormat(format).format(new Date(time.getTime()));
	}
}
