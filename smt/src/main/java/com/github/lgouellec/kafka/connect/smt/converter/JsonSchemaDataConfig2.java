package com.github.lgouellec.kafka.connect.smt.converter;

import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.connect.schema.AbstractDataConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.DecimalFormat;

import java.util.*;
import java.util.stream.Collectors;

public class JsonSchemaDataConfig2 extends AbstractDataConfig {

    public static final String ENFORCE_REQUIRED_FIELDS_CONFIG = "use.required.fields";
    public static final boolean ENFORCE_REQUIRED_FIELDS_DEFAULT = false;
    public static final String ENFORCE_REQUIRED_FIELDS_DOC =
            "Whether to set required for none optional fields.";

    public static final String OBJECT_ADDITIONAL_PROPERTIES_CONFIG = "object.additional.properties";
    public static final boolean OBJECT_ADDITIONAL_PROPERTIES_DEFAULT = true;
    public static final String OBJECT_ADDITIONAL_PROPERTIES_DOC =
            "Whether to allow additional properties for object schemas.";

    public static final String USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG = "use.optional.for.nonrequired";
    public static final boolean USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT = false;
    public static final String USE_OPTIONAL_FOR_NON_REQUIRED_DOC =
            "Whether to set non-required properties to be optional.";

    public static final String IGNORE_MODERN_DIALECTS_CONFIG = "ignore.modern.dialects";
    public static final boolean IGNORE_MODERN_DIALECTS_DEFAULT = false;
    public static final String IGNORE_MODERN_DIALECTS_DOC = "Whether to ignore modern dialects "
            + "of JSON Schema after draft 7, in which case draft 7 will be used.";

    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
    public static final String DECIMAL_FORMAT_DOC =
            "Controls which format this converter will serialize decimals in."
                    + " This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";

    public static final String FLATTEN_SINGLETON_UNIONS_CONFIG = "flatten.singleton.unions";
    public static final boolean FLATTEN_SINGLETON_UNIONS_DEFAULT = true;
    public static final String FLATTEN_SINGLETON_UNIONS_DOC = "Whether to flatten singleton unions";

    public static ConfigDef baseConfigDef() {
        return AbstractDataConfig.baseConfigDef()
                .define(
                        ENFORCE_REQUIRED_FIELDS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        ENFORCE_REQUIRED_FIELDS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        ENFORCE_REQUIRED_FIELDS_DOC
                )
        .define(
                OBJECT_ADDITIONAL_PROPERTIES_CONFIG,
                ConfigDef.Type.BOOLEAN,
                OBJECT_ADDITIONAL_PROPERTIES_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                OBJECT_ADDITIONAL_PROPERTIES_DOC
        ).define(
                USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                USE_OPTIONAL_FOR_NON_REQUIRED_DOC
        ).define(
                IGNORE_MODERN_DIALECTS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                IGNORE_MODERN_DIALECTS_DEFAULT,
                ConfigDef.Importance.LOW,
                IGNORE_MODERN_DIALECTS_DOC
        ).define(
                DECIMAL_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                DECIMAL_FORMAT_DEFAULT,
                JsonSchemaDataConfig.CaseInsensitiveValidString.in(
                        DecimalFormat.BASE64.name(),
                        DecimalFormat.NUMERIC.name()),
                ConfigDef.Importance.LOW,
                DECIMAL_FORMAT_DOC
        ).define(
                FLATTEN_SINGLETON_UNIONS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                FLATTEN_SINGLETON_UNIONS_DEFAULT,
                ConfigDef.Importance.LOW,
                FLATTEN_SINGLETON_UNIONS_DOC);
    }

    public JsonSchemaDataConfig2(Map<?, ?> props) {
        super(baseConfigDef(), props);
    }

    public boolean allowAdditionalProperties() {
        return getBoolean(OBJECT_ADDITIONAL_PROPERTIES_CONFIG);
    }

    public boolean useOptionalForNonRequiredProperties() {
        return getBoolean(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG);
    }

    public boolean useRequiredFields(){
        return getBoolean(ENFORCE_REQUIRED_FIELDS_CONFIG);
    }

    /**
     * Get the serialization format for decimal types.
     *
     * @return the decimal serialization format
     */
    public DecimalFormat decimalFormat() {
        return DecimalFormat.valueOf(getString(DECIMAL_FORMAT_CONFIG).toUpperCase(Locale.ROOT));
    }

    public boolean ignoreModernDialects() {
        return getBoolean(IGNORE_MODERN_DIALECTS_CONFIG);
    }

    public boolean isFlattenSingletonUnions() {
        return this.getBoolean(FLATTEN_SINGLETON_UNIONS_CONFIG);
    }

    public static class Builder {

        private final Map<String, Object> props = new HashMap<>();

        public JsonSchemaDataConfig2.Builder with(String key, Object value) {
            props.put(key, value);
            return this;
        }

        public JsonSchemaDataConfig2 build() {
            return new JsonSchemaDataConfig2(props);
        }
    }

    public static class CaseInsensitiveValidString implements ConfigDef.Validator {

        final Set<String> validStrings;

        private CaseInsensitiveValidString(List<String> validStrings) {
            this.validStrings = validStrings.stream()
                    .map(s -> s.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
        }

        public static JsonSchemaDataConfig2.CaseInsensitiveValidString in(String... validStrings) {
            return new JsonSchemaDataConfig2.CaseInsensitiveValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
                throw new ConfigException(name, o, "String must be one of (case insensitive): "
                        + String.join(", ", validStrings));
            }
        }

        public String toString() {
            return "(case insensitive) [" + String.join(", ", validStrings) + "]";
        }
    }
}
