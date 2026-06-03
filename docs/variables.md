# MDAI Variables

MDAI Variables are a core concept in the MDAI ecosystem, serving as dynamic placeholders for values that can be managed, observed, and acted upon. They are defined within the `MdaiHub` custom resource and are stored in Valkey. These variables can be injected as environment variables into OTEL collectors, enabling dynamic configuration and behavior.

There are three main types of variables, distinguished by the `type` field:

1.  **Manual Variables**: Externally managed by users.
2.  **Computed Variables**: Internally managed by MDAI automation rules.
3.  **Meta Variables**: Derived from other manual and computed variables.

All variables share a common set of data types (`dataType`) and serialization options.

## Variable ConfigMaps

The operator manages multiple ConfigMaps for variables:

1. **`<mdaihub-name>-manual-variables`**: Legacy manual-variable map in the hub namespace.
   - `data` key: variable key
   - `data` value: variable `dataType`
2. **`<mdaihub-name>-variables-schema`**: Structured variable definition map for all variable types (`manual`, `computed`, `meta`) in the hub namespace.
   - `data` key: variable key
   - `data` value: JSON object containing:
     - `type`
     - `dataType`
     - `storageType`
     - optional `variableRefs`
     - optional `serializeAs`
   - This map stores definitions only, not runtime variable values.
3. **`<mdaihub-name>-variables`**: Serialized values consumed by OTEL collectors in collector namespaces.

## Variable Types

### 1. Manual Variables (`type: manual`)

Manual variables are controlled and updated by external systems or users directly through the Valkey interface. They are not part of an automation loop and are not modified by MDAI automation rules. They are useful for providing static or externally managed configuration to your system.

**Allowed Data Types**: `string`, `int`, `boolean`, `set`, `map`

**Example:**

```yaml
# In MdaiHub spec
variables:
  - key: MANUAL_CONFIG_VALUE
    type: manual
    dataType: string
    serializeAs:
      - name: OTEL_MANUAL_CONFIG_VALUE
```
### 2. Computed Variables (type: computed)
   Computed variables are managed by the MDAI operator as part of an automation loop. Their values are updated by automationRules defined in the MdaiHub. These rules react to conditions (e.g., Prometheus alerts) and execute actions that modify the state of computed variables.
   **Allowed Data Types**: string, int, boolean, set, map  
   Example:
```yaml
# In MdaiHub spec
variables:
- key: DYNAMIC_SAMPLING_RATE
  type: computed
  dataType: int
  serializeAs:
    - name: OTEL_SAMPLING_RATE
```
### 3. Meta Variables (type: meta)
   Meta variables derive their values from one or more other variables (manual or computed). They are used to create composite or aggregated views of state. The calculation is based on their specific dataType.  
   **Allowed Data Types**: metaHashSet, metaPriorityList 
## Variable Data Types (dataType)
The dataType field specifies the kind of data a variable holds.
### Scalar Types
   These types hold a single value.   
   `string`: A simple string of text. Can be used to store complex data like YAML or JSON as a string.   
   `int`: An integer value. Stored as a string in Valkey. **YAML/kubectl precision ceiling**: integer literals in CR YAML are decoded through `float64` by the standard kubectl client, so any `default:` value outside `±(2^53 − 1)` (`±9_007_199_254_740_991`) is silently rounded before the apiserver sees it. For defaults in that range, submit the value through the gateway POST endpoint instead, which accepts raw JSON and preserves full `int64` precision end-to-end.   
   `float`: A 64-bit floating-point value. Stored as a string in Valkey. Writers are responsible for using a canonical serialization (e.g., the shortest round-trippable form) so reconciliation diffs do not fire on equivalent values that happen to be formatted differently. **YAML overflow note**: float literals that overflow `float64` (e.g., `1e400`) are coerced to JSON strings by the kubectl client before the apiserver sees them, so the webhook rejects them as `"float expected: cannot unmarshal string into float64"` rather than as NaN/Inf. The underlying NaN/Inf rejection still applies when defaults are submitted through the gateway POST endpoint as raw JSON.   
   `boolean`: A boolean value. Stored as "0" (false) or "1" (true) in Valkey.   
### Collection Types
   These types hold multiple values.   
   `set`: An unordered collection of unique strings, leveraging Valkey's Set data structure. Useful for managing lists of items where uniqueness is important.   
   `map`: A collection of key-value pairs, leveraging Valkey's Hash data structure. Both keys and values are strings.

   **Rendering contract**: map values are emitted to the env-var ConfigMap as YAML strings — no opportunistic int/float reparse. A value like `"01234"` survives intact (no leading-zero loss), and a value like `"100"` is rendered as `"100"` (quoted). Modern OTel collectors decode their config with strict typing (`confmap.WeaklyTypedInput: false`), so a map value substituted into a typed numeric processor field is rejected. For mixed-type processor config, declare a `string` variable carrying a JSON blob instead: the collector's YAML parser resolves types from the literal JSON syntax (e.g. `default: '{"send_batch_size":100,"timeout":"15s"}'`), giving the user explicit per-value type control without renderer guessing.
### Meta Data Types
   These are special data types for meta variables.   
   `metaHashSet`: A lookup table. It takes an input/key variable and a lookup variable (which must be a map) and returns a string value from the map corresponding to the value of the key variable. Typical uses: feature flags, allowlists/denylists (when you only need membership), routing toggles, etc.   
   `metaPriorityList`: Takes a list of variableRefs and evaluates to the value of the first variable in the list that is not empty or null. Use cases: rule chains, routing preferences, fallbacks.
## Defaults (`default`)
   Manual variables may declare a `default:` value of the same shape as their `dataType`. The default is a read-time projection: it is never written to Valkey, materialized only when no stored value exists, and superseded by any subsequent write.

   **YAML null handling**: writing `default: null`, or including `null` as an entry inside a `default:` map or list, is **not** a way to express an explicit null value. The Kubernetes API server prunes null leaves from `apiextensions.JSON` fields before admission webhooks run, so the value never reaches the operator and is silently dropped. To express "no default", omit the field entirely. To express the literal four-character string `"null"`, quote it: `default: "null"`.

## Serialization (serializeAs)
   The serializeAs field defines how a variable's value is exposed to other components, typically as environment variables in an OTEL Collector. It is an array, allowing a single variable to be exposed in multiple ways or with different transformations.  
   `name`: The name of the environment variable.  
   `transformers`: An optional list of transformations to apply to the value before serialization. For example, a set can be joined into a single comma-separated string.  
   Example with Transformer:
````yaml
variables:
- key: ACTIVE_FEATURES
  type: computed
  dataType: set
  serializeAs:
    - name: OTEL_ACTIVE_FEATURES_CSV
      transformers:
        - type: join
          join:
          delimiter: ","
 ````
In this example, the ACTIVE_FEATURES set might contain ["featureA", "featureB"] in Valkey, but it will be injected as the environment variable OTEL_ACTIVE_FEATURES_CSV="featureA,featureB".

![variable_types.png](variable_types.png)
