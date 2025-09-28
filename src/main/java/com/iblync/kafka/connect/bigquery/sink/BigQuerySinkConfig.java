//package com.iblync.kafka.connect.bigquery.sink;
//
//import static java.lang.String.format;
//
//import java.util.List;
//import java.util.Map;
//
//import org.apache.kafka.common.config.AbstractConfig;
//import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.common.config.ConfigDef.Importance;
//import org.apache.kafka.common.config.ConfigDef.Type;
//import org.apache.kafka.common.config.ConfigDef.Width;
//import org.apache.kafka.common.config.ConfigValue;
//
//import com.iblync.kafka.connect.bigquery.BigQuerySinkConnector;
//
//public class BigQuerySinkConfig extends AbstractConfig {
//
//	private static final String CONNECTOR_TYPE = "sink";
//	private static final String EMPTY_STRING = "";
//
//	public static final String TOPICS_CONFIG = BigQuerySinkConnector.TOPICS_CONFIG;
//	private static final String TOPICS_DOC = "A list of kafka topics for the sink connector, separated by commas";
//	public static final String TOPICS_DEFAULT = EMPTY_STRING;
//	private static final String TOPICS_DISPLAY = "The Kafka topics";
//	
//	public static final String TOPICS_REGEX_CONFIG = "topics.regex";
//	private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. "
//			+ "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " + "Only one of "
//			+ TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
//	private static final String TOPICS_REGEX_DEFAULT = EMPTY_STRING;
//	private static final String TOPICS_REGEX_DISPLAY = "Topics regex";
//	
//	
//	private static ConfigDef createConfigDef() {
//		ConfigDef configDef = new ConfigDef() {
//
//			@Override
//			@SuppressWarnings("unchecked")
//			public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
//				Map<String, ConfigValue> results = super.validateAll(props);
//				// Don't validate child configs if the top level configs are broken
//				if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
//					return results;
//				}
//
//				boolean hasTopicsConfig = !props.getOrDefault(TOPICS_CONFIG, "").trim().isEmpty();
//				boolean hasTopicsRegexConfig = !props.getOrDefault(TOPICS_REGEX_CONFIG, "").trim().isEmpty();
//
//				if (hasTopicsConfig && hasTopicsRegexConfig) {
//					results.get(TOPICS_CONFIG)
//							.addErrorMessage(format("%s and %s are mutually exclusive options, but both are set.",
//									TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
//				} else if (!hasTopicsConfig && !hasTopicsRegexConfig) {
//					results.get(TOPICS_CONFIG).addErrorMessage(
//							format("Must configure one of %s or %s", TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
//				}
//
//				return results;
//			}
//		};
//		String group = "Connection";
//		int orderInGroup = 0;
//		configDef.define(TOPICS_CONFIG, Type.LIST, TOPICS_DEFAULT, Importance.HIGH, TOPICS_DOC, group, ++orderInGroup,
//				Width.MEDIUM, TOPICS_DISPLAY);
//		
//		configDef.define(TOPICS_REGEX_CONFIG, Type.STRING, TOPICS_REGEX_DEFAULT, Validators.isAValidRegex(),
//				Importance.HIGH, TOPICS_REGEX_DOC, group, ++orderInGroup, Width.MEDIUM, TOPICS_REGEX_DISPLAY);
//
//
//		return configDef;
//	}
//
//	public BigQuerySinkConfig(ConfigDef definition, Map<?, ?> originals) {
//		super(definition, originals);
//		// TODO Auto-generated constructor stub
//	}
//
//}
