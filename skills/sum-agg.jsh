// sum-agg.jsh
// OpenSearch-style sum aggregation for Lucene indexes
// https://docs.opensearch.org/latest/aggregations/metric/sum/
//
// Usage:
//   jshell> var indexPath = "myidx"
//   jshell> /open skills/codec-support.jsh  (optional, for custom codec support)
//   jshell> /open skills/sum-agg.jsh
//   jshell> sum("mileage")                         // long field (default)
//   jshell> sum("price", "double")                 // double field (uses longBitsToDouble)
//   jshell> sum("price", "double", 0.0)            // with missing value
//   jshell> sumWithScript("price", "_value * 100", "double")
//   jshell> sumWithScript("price", "_value * 100", "double", 0.0)

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.function.DoubleUnaryOperator;

// Cached reader
DirectoryReader _sumReader = null;
Directory _sumDir = null;

// Codec error detection helper
String _detectCodecErrorSum(Exception e) {
    String msg = e.getMessage();
    if (msg == null) msg = "";
    Throwable cause = e.getCause();
    while (cause != null) {
        if (cause.getMessage() != null) msg += " " + cause.getMessage();
        cause = cause.getCause();
    }
    var pattern = Pattern.compile("Could not load codec '([^']+)'");
    var matcher = pattern.matcher(msg);
    if (matcher.find()) return matcher.group(1);
    return null;
}

void _openSumIndex() throws Exception {
    if (_sumReader != null) return;
    try {
        _sumDir = FSDirectory.open(Path.of(indexPath));
        _sumReader = DirectoryReader.open(_sumDir);
        System.out.println("Opened index: " + indexPath + " (maxDoc: " + _sumReader.maxDoc() + ")");
    } catch (Exception e) {
        String codecName = _detectCodecErrorSum(e);
        if (codecName != null) {
            System.out.println("\n" + "!".repeat(60));
            System.out.println("ERROR: Codec '" + codecName + "' not found!");
            System.out.println("!".repeat(60));
            System.out.println("\nTo fix this:");
            System.out.println("1. Load codec support:  /open skills/codec-support.jsh");
            System.out.println("2. Register the codec:  registerCodec(\"" + codecName + "\", \"/path/to/codec.jar\")");
            System.out.println("3. Run the /env command shown, then reload this skill");
            throw new RuntimeException("Codec not found: " + codecName);
        }
        throw e;
    }
}

void closeSumIndex() throws Exception {
    if (_sumReader != null) {
        _sumReader.close();
        _sumReader = null;
    }
    if (_sumDir != null) {
        _sumDir.close();
        _sumDir = null;
    }
    System.out.println("Index closed.");
}

// Simple expression parser for scripts like "_value * 100", "_value + 10", etc.
DoubleUnaryOperator parseScript(String script) {
    if (script == null || script.trim().isEmpty()) {
        return v -> v;
    }

    String s = script.trim();

    // Pattern: _value <op> <number> or <number> <op> _value
    var pattern = Pattern.compile("_value\\s*([+\\-*/])\\s*([\\d.]+)");
    var matcher = pattern.matcher(s);
    if (matcher.matches()) {
        String op = matcher.group(1);
        double num = Double.parseDouble(matcher.group(2));
        return switch (op) {
            case "*" -> v -> v * num;
            case "/" -> v -> v / num;
            case "+" -> v -> v + num;
            case "-" -> v -> v - num;
            default -> v -> v;
        };
    }

    // Pattern: <number> <op> _value
    var pattern2 = Pattern.compile("([\\d.]+)\\s*([+\\-*/])\\s*_value");
    var matcher2 = pattern2.matcher(s);
    if (matcher2.matches()) {
        double num = Double.parseDouble(matcher2.group(1));
        String op = matcher2.group(2);
        return switch (op) {
            case "*" -> v -> num * v;
            case "/" -> v -> num / v;
            case "+" -> v -> num + v;
            case "-" -> v -> num - v;
            default -> v -> v;
        };
    }

    // Pattern: Math.sqrt(_value), Math.abs(_value), etc.
    if (s.equals("Math.sqrt(_value)")) return Math::sqrt;
    if (s.equals("Math.abs(_value)")) return Math::abs;
    if (s.equals("Math.floor(_value)")) return Math::floor;
    if (s.equals("Math.ceil(_value)")) return Math::ceil;
    if (s.equals("Math.round(_value)")) return v -> (double) Math.round(v);
    if (s.equals("-_value") || s.equals("-1 * _value")) return v -> -v;
    if (s.equals("_value")) return v -> v;

    // Pattern: Math.pow(_value, N)
    var powPattern = Pattern.compile("Math\\.pow\\(_value,\\s*([\\d.]+)\\)");
    var powMatcher = powPattern.matcher(s);
    if (powMatcher.matches()) {
        double exp = Double.parseDouble(powMatcher.group(1));
        return v -> Math.pow(v, exp);
    }

    throw new IllegalArgumentException("Unsupported script: " + script +
        "\nSupported: _value <op> N, Math.sqrt(_value), Math.abs(_value), Math.pow(_value, N)");
}

// Check if field exists and is numeric
boolean _isNumericField(String field) throws Exception {
    for (LeafReaderContext leaf : _sumReader.leaves()) {
        FieldInfo finfo = leaf.reader().getFieldInfos().fieldInfo(field);
        if (finfo != null) {
            DocValuesType dvType = finfo.getDocValuesType();
            return dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC;
        }
    }
    return false;
}

// Convert raw long value based on field type
double _convertValue(long rawValue, String type) {
    if (type == null) type = "long";
    return switch (type.toLowerCase()) {
        case "double", "d", "float", "f" -> Double.longBitsToDouble(rawValue);
        case "int", "i", "long", "l" -> (double) rawValue;
        default -> (double) rawValue;
    };
}

// Check if type requires Kahan summation (floating point types)
boolean _useKahanSummation(String type) {
    if (type == null) return false;
    return switch (type.toLowerCase()) {
        case "double", "d", "float", "f" -> true;
        default -> false;
    };
}

// Core sum implementation with type support
void _sumImpl(String field, String type, Double missing, String script) throws Exception {
    _openSumIndex();

    // Default type to "long"
    if (type == null) type = "long";

    // Validate field
    boolean fieldExists = false;
    for (LeafReaderContext leaf : _sumReader.leaves()) {
        if (leaf.reader().getFieldInfos().fieldInfo(field) != null) {
            fieldExists = true;
            break;
        }
    }

    if (!fieldExists) {
        System.out.println("Error: field '" + field + "' does not exist");
        return;
    }

    if (!_isNumericField(field)) {
        System.out.println("Error: field '" + field + "' is not a numeric docvalues field");
        return;
    }

    // Parse script
    DoubleUnaryOperator transform;
    try {
        transform = parseScript(script);
    } catch (IllegalArgumentException e) {
        System.out.println("Error: " + e.getMessage());
        return;
    }

    // Aggregate with optional Kahan summation for precision
    boolean useKahan = _useKahanSummation(type);
    double sum = 0.0;
    double kahanC = 0.0;  // Kahan compensation
    long count = 0;
    long missingCount = 0;

    for (LeafReaderContext leaf : _sumReader.leaves()) {
        LeafReader reader = leaf.reader();
        NumericDocValues ndv = reader.getNumericDocValues(field);

        if (ndv == null) {
            // Field doesn't exist in this segment
            if (missing != null) {
                int maxDoc = reader.maxDoc();
                for (int doc = 0; doc < maxDoc; doc++) {
                    // Check if doc is live
                    if (reader.getLiveDocs() == null || reader.getLiveDocs().get(doc)) {
                        double value = transform.applyAsDouble(missing);
                        if (useKahan) {
                            double y = value - kahanC;
                            double t = sum + y;
                            kahanC = (t - sum) - y;
                            sum = t;
                        } else {
                            sum += value;
                        }
                        missingCount++;
                    }
                }
            }
            continue;
        }

        int maxDoc = reader.maxDoc();
        for (int doc = 0; doc < maxDoc; doc++) {
            // Check if doc is live
            if (reader.getLiveDocs() != null && !reader.getLiveDocs().get(doc)) {
                continue;
            }

            if (ndv.advanceExact(doc)) {
                double value = _convertValue(ndv.longValue(), type);
                value = transform.applyAsDouble(value);
                if (useKahan) {
                    // Kahan summation for precision
                    double y = value - kahanC;
                    double t = sum + y;
                    kahanC = (t - sum) - y;
                    sum = t;
                } else {
                    sum += value;
                }
                count++;
            } else if (missing != null) {
                double value = transform.applyAsDouble(missing);
                if (useKahan) {
                    double y = value - kahanC;
                    double t = sum + y;
                    kahanC = (t - sum) - y;
                    sum = t;
                } else {
                    sum += value;
                }
                missingCount++;
            }
        }
    }

    // Output
    System.out.println();
    System.out.println("Sum Aggregation: " + field);
    System.out.println("=".repeat(40));
    System.out.println("Field:        " + field);
    System.out.println("Type:         " + type + (useKahan ? " (Kahan summation)" : ""));
    System.out.println("Count:        " + count);
    if (missing != null) {
        System.out.println("Missing:      " + missingCount + " (used value: " + missing + ")");
    }
    if (script != null && !script.isEmpty()) {
        System.out.println("Script:       " + script);
    }
    System.out.println("-".repeat(40));
    System.out.println("Sum:          " + sum);
    System.out.println("=".repeat(40));
}

// Public API functions

// Basic sum (type defaults to long)
void sum(String field) throws Exception {
    _sumImpl(field, "long", null, null);
}

// Sum with type
void sum(String field, String type) throws Exception {
    _sumImpl(field, type, null, null);
}

// Sum with type and missing value
void sum(String field, String type, double missing) throws Exception {
    _sumImpl(field, type, missing, null);
}

// Sum with script (type defaults to long)
void sumWithScript(String field, String script) throws Exception {
    _sumImpl(field, "long", null, script);
}

// Sum with script and type
void sumWithScript(String field, String script, String type) throws Exception {
    _sumImpl(field, type, null, script);
}

// Sum with script, type, and missing value
void sumWithScript(String field, String script, String type, double missing) throws Exception {
    _sumImpl(field, type, missing, script);
}

System.out.println("Loaded sum-agg skill (OpenSearch-style sum aggregation)");
System.out.println();
System.out.println("Usage:");
System.out.println("  sum(\"field\")                              - long field (default)");
System.out.println("  sum(\"field\", \"double\")                    - double field (longBitsToDouble)");
System.out.println("  sum(\"field\", \"double\", 0.0)               - with missing value");
System.out.println("  sumWithScript(\"field\", \"_value * 100\")");
System.out.println("  sumWithScript(\"field\", \"_value * 100\", \"double\")");
System.out.println("  sumWithScript(\"field\", \"_value * 100\", \"double\", 0.0)");
System.out.println();
System.out.println("Field types:");
System.out.println("  \"long\"/\"l\"   - raw long value (default)");
System.out.println("  \"int\"/\"i\"    - cast to double");
System.out.println("  \"double\"/\"d\" - Double.longBitsToDouble() + Kahan summation");
System.out.println("  \"float\"/\"f\"  - same as double");
System.out.println();
System.out.println("Supported scripts:");
System.out.println("  _value * N, _value + N, _value / N, _value - N");
System.out.println("  Math.sqrt(_value), Math.abs(_value), Math.pow(_value, N)");
System.out.println();
System.out.println("  closeSumIndex() - close when done");
