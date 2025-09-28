//package com.iblync.kafka.connect.bigquery.sink.config;
//
//import java.lang.reflect.Method;
//import java.lang.reflect.Proxy;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.common.config.types.Password;
//
//import com.iblync.kafka.connect.bigquery.sink.handler.CustomTypeHandler;
//import com.iblync.kafka.connect.bigquery.util.Default;
//import com.iblync.kafka.connect.bigquery.util.DefaultList;
//
//public class KafkaConfigProxyFactory {
//	protected final String prefix;
//	protected final Map<Class<?>, CustomTypeHandler<?>> customTypeMap = new HashMap<>();
//	protected final Map<Class<?>, ConfigDef.Type> javaClassToKafkaType = new HashMap<>();
//
//	public KafkaConfigProxyFactory(String prefix) {
//		// ensure prefix ends with dot
//		this.prefix = prefix.isEmpty() ? "" : (prefix.endsWith(".") ? prefix : prefix + ".");
//		initTypeMap();
//	}
//
//	protected void initTypeMap() {
//		javaClassToKafkaType.put(Boolean.class, ConfigDef.Type.BOOLEAN);
//		javaClassToKafkaType.put(Boolean.TYPE, ConfigDef.Type.BOOLEAN);
//		javaClassToKafkaType.put(String.class, ConfigDef.Type.STRING);
//		javaClassToKafkaType.put(Integer.class, ConfigDef.Type.INT);
//		javaClassToKafkaType.put(Integer.TYPE, ConfigDef.Type.INT);
//		javaClassToKafkaType.put(Short.class, ConfigDef.Type.SHORT);
//		javaClassToKafkaType.put(Short.TYPE, ConfigDef.Type.SHORT);
//		javaClassToKafkaType.put(Long.class, ConfigDef.Type.LONG);
//		javaClassToKafkaType.put(Long.TYPE, ConfigDef.Type.LONG);
//		javaClassToKafkaType.put(Double.class, ConfigDef.Type.DOUBLE);
//		javaClassToKafkaType.put(Double.TYPE, ConfigDef.Type.DOUBLE);
//		javaClassToKafkaType.put(List.class, ConfigDef.Type.LIST);
//		javaClassToKafkaType.put(Class.class, ConfigDef.Type.CLASS);
//		javaClassToKafkaType.put(Password.class, ConfigDef.Type.PASSWORD);
//	}
//
//	@SuppressWarnings("unchecked")
//	public <T> T newProxy(Class<T> configInterface, Map<String, String> props) {
//		return (T) Proxy.newProxyInstance(configInterface.getClassLoader(), new Class[] { configInterface },
//				(proxy, method, args) -> {
//					if (method.getDeclaringClass() == Object.class) {
//						return method.invoke(this, args);
//					}
//
//					String key;
//					Object rawValue;
//
//					if (args != null && args.length == 1 && args[0] instanceof String) {
//						String topic = (String) args[0];
//						key = prefix + "topic.to." + topic;
//						rawValue = props.get(key);
//					} else {
//						key = prefix + toConfigKey(method.getName());
//						rawValue = props.get(key);
//					}
//
//					if (rawValue == null) {
//						rawValue = getDefault(method);
//					}
//
//					return convertValue(method, rawValue);
//				});
//	}
//
//	private String toConfigKey(String methodName) {
//		// camelCase -> dot.case
//		return methodName.replaceAll("([a-z])([A-Z]+)", "$1.$2").toLowerCase();
//	}
//
//	private Object getDefault(Method method) {
//	    if (method.isAnnotationPresent(Default.class)) {
//	        String raw = method.getAnnotation(Default.class).value();
//	        Class<?> returnType = method.getReturnType();
//
//	        if (List.class.isAssignableFrom(returnType)) {
//	            if (raw.isEmpty()) {
//	                return List.of(); 
//	            }
//	            return Arrays.asList(raw.split(","));
//	        }
//
//	        // String, int, boolean, etc.
//	        if (returnType.equals(Boolean.class) || returnType.equals(boolean.class)) {
//	            return Boolean.parseBoolean(raw);
//	        }
//	        if (returnType.equals(Integer.class) || returnType.equals(int.class)) {
//	            return Integer.parseInt(raw);
//	        }
//	        if (returnType.equals(Long.class) || returnType.equals(long.class)) {
//	            return Long.parseLong(raw);
//	        }
//
//	        return raw; // default to string
//	    }
//	    return null;
//	}
//
//	private Object convertValue(Method method, Object rawValue) {
//		if (rawValue == null)
//			return null;
//
//		Class<?> returnType = method.getReturnType();
//		ConfigDef.Type kafkaType = javaClassToKafkaType.get(returnType);
//
//		if (kafkaType == null) {
//			// fallback: return as-is
//			return rawValue;
//		}
//
//		String value = rawValue.toString();
//		switch (kafkaType) {
//		case BOOLEAN:
//			return Boolean.parseBoolean(value);
//		case INT:
//			return Integer.parseInt(value);
//		case SHORT:
//			return Short.parseShort(value);
//		case LONG:
//			return Long.parseLong(value);
//		case DOUBLE:
//			return Double.parseDouble(value);
//		case LIST:
//			return Arrays.asList(value.split(","));
//		case CLASS:
//			try {
//				return Class.forName(value);
//			} catch (ClassNotFoundException e) {
//				throw new IllegalArgumentException("Invalid class: " + value, e);
//			}
//		case PASSWORD:
//			return new Password(value);
//		case STRING:
//		default:
//			return value;
//		}
//	}
//}

package com.iblync.kafka.connect.bigquery.sink.config;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import com.iblync.kafka.connect.bigquery.util.Default;

public class KafkaConfigProxyFactory {

	private final String prefix;
	private final Map<Class<?>, ConfigDef.Type> javaClassToKafkaType;

	public KafkaConfigProxyFactory(String prefix) {
		this.prefix = prefix.isEmpty() ? "" : (prefix.endsWith(".") ? prefix : prefix + ".");
		this.javaClassToKafkaType = Map.of(Boolean.class, ConfigDef.Type.BOOLEAN, Boolean.TYPE, ConfigDef.Type.BOOLEAN,
				String.class, ConfigDef.Type.STRING, Integer.class, ConfigDef.Type.INT, Integer.TYPE,
				ConfigDef.Type.INT, Long.class, ConfigDef.Type.LONG, Long.TYPE, ConfigDef.Type.LONG, Double.class,
				ConfigDef.Type.DOUBLE, Double.TYPE, ConfigDef.Type.DOUBLE, List.class, ConfigDef.Type.LIST);
	}

	@SuppressWarnings("unchecked")
	public <T> T newProxy(Class<T> configInterface, Map<String, String> props) {
		return (T) Proxy.newProxyInstance(configInterface.getClassLoader(), new Class[] { configInterface },
				(proxy, method, args) -> {
					if (method.getDeclaringClass() == Object.class) {
						return method.invoke(this, args);
					}

					String key = prefix + toConfigKey(method.getName());
					String rawValue = props.get(key);

					if (rawValue == null) {
						rawValue = getDefaultValue(method);
					}

					return convertValue(method, rawValue);
				});
	}

	private String toConfigKey(String methodName) {
		// Convert camelCase to dot.case: topicToTable -> topic.to.table
		return methodName.replaceAll("([a-z])([A-Z]+)", "$1.$2").toLowerCase(Locale.ROOT);
	}

	private String getDefaultValue(Method method) {
		if (method.isAnnotationPresent(Default.class)) {
			return method.getAnnotation(Default.class).value();
		}
		return null;
	}

	private Object convertValue(Method method, String rawValue) {
		if (rawValue == null)
			return null;

		Class<?> returnType = method.getReturnType();

		if (List.class.isAssignableFrom(returnType)) {
			return rawValue.isEmpty() ? List.of() : Arrays.asList(rawValue.split(","));
		}
		if (Boolean.class.equals(returnType) || boolean.class.equals(returnType)) {
			return Boolean.parseBoolean(rawValue);
		}
		if (Integer.class.equals(returnType) || int.class.equals(returnType)) {
			return Integer.parseInt(rawValue);
		}
		if (Long.class.equals(returnType) || long.class.equals(returnType)) {
			return Long.parseLong(rawValue);
		}
		if (Double.class.equals(returnType) || double.class.equals(returnType)) {
			return Double.parseDouble(rawValue);
		}

		return rawValue; // default: string
	}

	public <T> ConfigDef define(Class<T> configInterface) {
		ConfigDef def = new ConfigDef();

		for (Method method : configInterface.getMethods()) {
			if (Modifier.isStatic(method.getModifiers()))
				continue;

			String key = prefix + toConfigKey(method.getName());
			ConfigDef.Type type = getKafkaType(method);
			Object defaultValue = getDefaultValue(method);

			def.define(key, type, defaultValue, ConfigDef.Importance.MEDIUM,
					"Automatically generated config key for " + method.getName());
		}

		return def;
	}

	private ConfigDef.Type getKafkaType(Method method) {
		Class<?> type = method.getReturnType();
		if (javaClassToKafkaType.containsKey(type)) {
			return javaClassToKafkaType.get(type);
		}
		if (type.isEnum()) {
			return ConfigDef.Type.STRING;
		}
		throw new IllegalArgumentException("Unsupported return type for method: " + method);
	}

}
