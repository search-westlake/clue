# Clue JShell Skills

JShell scripts for analyzing Lucene indexes using pure Lucene API.

## Usage

```bash
# Start jshell with Lucene on classpath
jshell --class-path "build/libs/*"

# Set index path
jshell> var indexPath = "/path/to/index"

# Load a skill
jshell> /open skills/<skill-name>.jsh
```

## Available Skills

### field-info-load.jsh
Loads field info from all segments into a cache file for querying.

```java
var indexPath = "myidx"
/open skills/field-info-load.jsh
// Output: field-info-cache.json
```

### field-info-query.jsh
Query functions for cached field info. Run after `field-info-load.jsh`.

```java
/open skills/field-info-query.jsh

summary()                  // index stats by docval type, index options
listFields()               // list all field names
listFields("user.*")       // list fields matching regex pattern
listFields(".*", 50)       // list with custom limit
fieldInfo("fieldName")     // full details for a specific field
```

### top-terms.jsh
Output top N terms for a field ordered by docFreq descending.

```java
var indexPath = "myidx"
/open skills/top-terms.jsh

topTerms("fieldName", 10)  // top 10 terms by docFreq
closeIndex()               // close when done
```

Returns error if field is not indexed (no term dictionary).

### doc-values.jsh
Output all field values for a given docid (stored fields + docvalues).

```java
var indexPath = "myidx"
/open skills/doc-values.jsh

docValues(0)               // show all values for doc 0
docValues(100)             // show all values for doc 100
closeDocIndex()            // close when done
```

- Skips fields with no values for the document
- For binary data: attempts UTF-8 decode, falls back to base64
- Handles all docvalue types: NUMERIC, BINARY, SORTED, SORTED_SET, SORTED_NUMERIC

### sum-agg.jsh
OpenSearch-style sum aggregation for numeric fields.
See: https://docs.opensearch.org/latest/aggregations/metric/sum/

```java
var indexPath = "myidx"
/open skills/sum-agg.jsh

// All documents
sum("mileage")                                  // long field (default)
sum("price", "double")                          // double field (uses longBitsToDouble)
sum("price", "double", 0.0)                     // with missing value
sumWithScript("price", "_value * 100", "double")

// Explicit docid list filter
sumDocs("mileage", new int[]{0, 1, 5, 10, 100})
sumDocs("price", "double", new int[]{0, 1, 5})
sumDocsWithScript("price", "_value * 100", "double", new int[]{0, 1})

// Query filter (uses Lucene QueryParser syntax)
sumQuery("mileage", "color_indexed:red")
sumQuery("price", "double", "year:[2000 TO *]")
sumQueryWithScript("price", "_value * 100", "double", "color_indexed:red AND year:>2000")

closeSumIndex()                                 // close when done
```

**Field types:**
| Type | Handling |
|------|----------|
| `"long"` / `"l"` | Raw long value (default) |
| `"int"` / `"i"` | Cast to double |
| `"double"` / `"d"` | `Double.longBitsToDouble()` + Kahan summation |
| `"float"` / `"f"` | Same as double |

**Supported scripts:**
- Arithmetic: `_value * N`, `_value + N`, `_value / N`, `_value - N`
- Math functions: `Math.sqrt(_value)`, `Math.abs(_value)`, `Math.pow(_value, N)`

**Parameters (matching OpenSearch API):**
| Parameter | Description |
|-----------|-------------|
| `field` | The numeric docvalues field to sum |
| `type` | Field type: `"long"` (default), `"double"`, `"float"`, `"int"` |
| `missing` | Default value for documents missing the field |
| `script` | Expression to transform values (uses `_value` variable) |

**Document filtering:**
| Method | Description |
|--------|-------------|
| `sum()` / `sumWithScript()` | Aggregate over all documents |
| `sumDocs()` / `sumDocsWithScript()` | Aggregate over explicit docid list |
| `sumQuery()` / `sumQueryWithScript()` | Aggregate over documents matching a query |

**Query syntax:** Uses Lucene QueryParser with StandardAnalyzer. Examples:
- `"field:value"` - term query
- `"field:[min TO max]"` - range query
- `"field:>100"` - range query (greater than)
- `"field1:a AND field2:b"` - boolean query
- `"*"` - match all documents

**Note:** Double/float types use Kahan summation algorithm to maintain precision when summing many floating-point values.

### codec-support.jsh
Utilities for handling indexes built with custom codecs.

```java
/open skills/codec-support.jsh

registerCodec("MyCodec", "/path/to/codec.jar")  // register a codec JAR
```

## Custom Codec Support

If your index was built with a custom codec not included in the standard Lucene JARs, the skills will detect this and provide instructions.

### Workflow

1. **Error Detection**: When a skill tries to open an index with a missing codec, you'll see:
   ```
   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   ERROR: Codec 'MyCustomCodec' not found!
   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   ```

2. **Register the Codec**: Load codec-support and register your codec JAR:
   ```java
   /open skills/codec-support.jsh
   registerCodec("MyCustomCodec", "/path/to/my-codec.jar")
   ```

3. **Update Classpath**: Run the `/env` command shown:
   ```java
   /env -class-path build/libs/*:/path/to/my-codec.jar
   ```

4. **Reload the Skill**: Re-run `/open skills/<skill>.jsh`

### Config Persistence

Registered codecs are saved to `codec-config.json` in the working directory:
```json
{
  "codecs": {
    "MyCustomCodec": "/path/to/my-codec.jar"
  }
}
```

On subsequent sessions, `codec-support.jsh` will load this config and suggest the appropriate classpath.

### Example: Using a Custom Codec

```bash
# Start jshell
jshell --class-path "build/libs/*"

# Try to open an index with custom codec - will fail
jshell> var indexPath = "/path/to/custom-index"
jshell> /open skills/field-info-load.jsh
# ERROR: Codec 'MyCodec' not found!

# Register the codec
jshell> /open skills/codec-support.jsh
jshell> registerCodec("MyCodec", "plugins/my-codec.jar")
# Run: /env -class-path build/libs/*:plugins/my-codec.jar

# Update classpath
jshell> /env -class-path build/libs/*:plugins/my-codec.jar

# Now reload the skill - it will work
jshell> /open skills/field-info-load.jsh
# Success!
```
