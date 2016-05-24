package org.tarantool.named;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.tarantool.AbstractTarantoolConnection16;
import org.tarantool.Code;
import org.tarantool.Key;

public abstract class TarantoolNamedBase16<R> extends AbstractTarantoolConnection16<String, Map<String, Object>, UpdateOperation, R> {
    protected static final int ER_SCHEMA_CHANGED = 32877;
    protected static final int VSPACE = 281;
    protected static final int VINDEX = 289;
    protected Map<String, Integer> schema;
    protected Map<String, String> fields;

    protected String toFieldName(String spaceName, String fieldName) {
        return "field." + spaceName + "." + fieldName;
    }


    protected String toKeyName(String spaceName, String keyName) {
        return "key." + spaceName + "." + keyName;
    }

    protected String toSpaceName(String spaceName) {
        return "space." + spaceName;
    }

    protected String toSpaceSizeName(String name) {
        return "space." + name + ".size";
    }

    protected Object[] resolveKeyValue(String spaceName, Map<String, Object> value) {
        Map<String, Object> map = value;
        TreeMap<Integer, Object> idxVal = new TreeMap<Integer, Object>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Integer idx = resolveField(spaceName, entry.getKey());
            idxVal.put(idx, entry.getValue());
        }
        return idxVal.values().toArray(new Object[value.size()]);
    }

    protected Object[] resolveUpdateOps(String spaceName, UpdateOperation[] value) {
        UpdateOperation[] ops = value;
        Object[] converted = new Object[ops.length];
        int opIdx = 0;
        for (UpdateOperation up : ops) {
            Object[] res = new Object[2 + up.getArguments().length];
            int a = 0;
            res[a++] = up.getOp();
            res[a++] = resolveField(spaceName, up.getField());
            for (Object arg : up.getArguments()) {
                res[a++] = arg;
            }
            converted[opIdx++] = res;
        }
        return converted;
    }


    protected Integer resolveField(String spaceName, String fieldName) {
        Integer idx = schema.get(toFieldName(spaceName, fieldName));
        if (idx == null) {
            throw new IllegalArgumentException("unknown field " + spaceName + " " + fieldName);
        }
        return idx;
    }


    protected Integer resolveKey(String spaceName, String keyName) {
        Integer idx = schema.get(toKeyName(spaceName, keyName));
        if (idx == null) {
            throw new IllegalArgumentException("unknown key " + spaceName + " " + keyName);
        }
        return idx;
    }


    protected int resolveSpace(String spaceName) {
        Integer idx = schema.get(toSpaceName(spaceName));
        if (idx == null) {
            throw new IllegalArgumentException("unknown space " + spaceName);
        }
        return idx;
    }


    protected Object[] resolveTuple(String spaceName, Map<String, Object> value) {
        Map<String, Object> map = value;
        Integer size = schema.get("space." + spaceName + ".size");
        Object[] tuple = new Object[size];
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Integer idx = resolveField(spaceName, entry.getKey());
            tuple[idx] = entry.getValue();
        }
        return tuple;
    }

    protected void buildSchema(List<List> spaces, List<List> indexes) {
        Map<String, Integer> schema = new HashMap<String, Integer>();
        Map<String, String> fields = new HashMap<String, String>();
        Map<Integer, String> spaceReverse = new HashMap<Integer, String>();
        for (List space : spaces) {
            Integer spaceIdx = (Integer) space.get(0);
            String name = (String) space.get(2);
            schema.put(toSpaceName(name), spaceIdx);
            spaceReverse.put(spaceIdx, name);
            final List spaceFieldsMap = (List) space.get(6);
            for (int i = 0; i < spaceFieldsMap.size(); i++) {
                Map<String, String> elem = (Map<String, String>) spaceFieldsMap.get(i);
                schema.put(toFieldName(name, elem.get("name")), i);
                fields.put(spaceIdx + "." + i, elem.get("name"));
            }
            schema.put(toSpaceSizeName(name), spaceFieldsMap.size());
        }
        for (List index : indexes) {
            Integer spaceIdx = (Integer) index.get(0);
            Integer idx = (Integer) index.get(1);
            String name = (String) index.get(2);
            schema.put(toKeyName(spaceReverse.get(spaceIdx), name), idx);
        }
        this.schema = schema;
        this.fields = fields;
    }


    protected List resolveTuples(Code code, Object[] args, List<List> tuples) {
        return resolveTuplesWithFields(code, args, tuples, this.fields);
    }

    protected List resolveTuplesWithFields(Code code, Object[] args, List<List> tuples, Map<String, String> fields) {
        List<Map<String, Object>> objects = new ArrayList<Map<String, Object>>(tuples.size());
        if (isCodeResolvable(code)) {
            int spaceIdx = -1;
            for (int i = 0, e = args.length; i < e; i += 2) {
                if(args[i] == Key.SPACE) {
                    spaceIdx = resolveSpace((String) args[i + 1]);
                    break;
                }
            }
            for (List tuple : tuples) {
                Map<String, Object> obj = new LinkedHashMap<String, Object>(tuple.size());
                for (int i = 0; i < tuple.size(); i++) {
                    obj.put(fields.get(spaceIdx + "." + i), tuple.get(i));
                }
                objects.add(obj);
            }
            return objects;
        }
        return tuples;
    }

    protected boolean isCodeResolvable(Code code) {
        return code.getId() >= Code.SELECT.getId() && code.getId() <= Code.DELETE.getId();
    }

    protected Object[] resolveArgs(Code code, Object[] args) {
        String spaceName = null;
        Object[] mutableArgs = new Object[args.length];
        for (int i = 0, e = args.length; i < e; i += 2) {
            Object value = args[i + 1];
            Key key = (Key) args[i];
            mutableArgs[i] = key;
            mutableArgs[i + 1] = value;
            if (key == Key.SPACE) {
                mutableArgs[i + 1] = resolveSpace(spaceName = (String) value);
            } else if (key == Key.KEY) {
                mutableArgs[i + 1] = resolveKeyValue(spaceName, (Map<String, Object>) value);
            } else if (key == Key.INDEX) {
                mutableArgs[i + 1] = resolveKey(spaceName, (String) value);
            } else if ((code == Code.UPDATE && key == Key.TUPLE) || (code == Code.UPSERT && key == Key.UPSERT_OPS)) {
                mutableArgs[i + 1] = resolveUpdateOps(spaceName, (UpdateOperation[]) value);
            } else if (key == Key.TUPLE && value instanceof Map) {
                mutableArgs[i + 1] = resolveTuple(spaceName, (Map<String, Object>) value);
            }
        }
        return mutableArgs;
    }


    public abstract long getSchemaId();


}
