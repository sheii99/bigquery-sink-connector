package com.iblync.kafka.connect.bigquery.sink.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Locale;
import java.util.Map;
import com.iblync.kafka.connect.bigquery.util.Default;


public class ConfigProxyHandler implements InvocationHandler {
    private final Map<String, String> props;

    public ConfigProxyHandler(Map<String, String> props) {
        this.props = props;
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> configInterface, Map<String, String> props) {
        return (T) Proxy.newProxyInstance(
                configInterface.getClassLoader(),
                new Class[]{configInterface},
                new ConfigProxyHandler(props));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Convert methodName -> dotted key
        String key = lowerCamelCaseToDottedLowerCase(method.getName());
        String rawValue = props.get(key);

        // If not in props, check @Default annotation
        if (rawValue == null && method.isAnnotationPresent(Default.class)) {
            rawValue = method.getAnnotation(Default.class).value();
        }

        // If still null, return Java default
        if (rawValue == null) {
            if (method.getReturnType().isPrimitive()) {
                if (method.getReturnType().equals(boolean.class)) return false;
                if (method.getReturnType().equals(int.class)) return 0;
                if (method.getReturnType().equals(long.class)) return 0L;
                if (method.getReturnType().equals(double.class)) return 0.0;
            }
            return null;
        }

        // Type conversion
        if (method.getReturnType().equals(String.class)) {
            return rawValue;
        }
        if (method.getReturnType().equals(int.class) || method.getReturnType().equals(Integer.class)) {
            return Integer.parseInt(rawValue);
        }
        if (method.getReturnType().equals(boolean.class) || method.getReturnType().equals(Boolean.class)) {
            return Boolean.parseBoolean(rawValue);
        }
        if (method.getReturnType().equals(long.class) || method.getReturnType().equals(Long.class)) {
            return Long.parseLong(rawValue);
        }
        if (method.getReturnType().equals(double.class) || method.getReturnType().equals(Double.class)) {
            return Double.parseDouble(rawValue);
        }

        throw new UnsupportedOperationException("Unsupported return type: " + method.getReturnType());
    }

    private static String lowerCamelCaseToDottedLowerCase(String name) {
        return name.replaceAll("(\\p{Upper})", ".$1").toLowerCase(Locale.ROOT);
    }
}
