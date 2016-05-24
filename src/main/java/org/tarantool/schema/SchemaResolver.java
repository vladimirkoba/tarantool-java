package org.tarantool.schema;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.TarantoolConnection16Ops;

public class SchemaResolver {

    public <T> T schema(T schema, TarantoolConnection16Ops<Integer,Object,Object,List> ops)  {
        final Map<String, List> spaces = callMap(ops, 281, new int[]{2}, "");
        final String idxSep = "_";
        final Map<String, List> indexes = callMap(ops, 289, new int[]{0, 2}, idxSep);
        final Field[] fields = schema.getClass().getFields();
        for (Field field : fields) {
            final Space space = field.getAnnotation(Space.class);
            if (space != null) {
                String spaceName = space.value().isEmpty() ? field.getName() : space.value();
                List spaceMeta = spaces.get(spaceName);
                final Integer spaceIndex = ((Number) spaceMeta.get(0)).intValue();
                if(spaceIndex == null) {
                    throw new IllegalStateException("Can't find ID for space "+spaceName);
                }
                try {
                    final Object spaceObject = field.get(schema);
                    for (Field f : spaceObject.getClass().getFields()) {
                        final SpaceId spaceId = f.getAnnotation(SpaceId.class);
                        final IndexId indexId = f.getAnnotation(IndexId.class);
                        final SchemaId schemaId = f.getAnnotation(SchemaId.class);
                        final FieldsMapping fieldsMapping = f.getAnnotation(FieldsMapping.class);
                        if (spaceId != null) {
                            f.set(spaceObject, f.getClass().isPrimitive() ? spaceIndex.intValue() : spaceIndex);
                        } else if(indexId!=null) {
                            final String indexName = indexId.value().isEmpty() ? f.getName() : indexId.value();
                            final Integer indexIdx = (Integer)indexes.get(spaceIndex + idxSep + indexName).get(1);
                            if(indexIdx == null) {
                                throw new IllegalStateException("Can't find index id " + spaceName + "." + indexName);
                            }
                            f.set(spaceObject, indexIdx);
                        } else if(fieldsMapping != null) {
                            final List spaceFieldsMap = (List) spaceMeta.get(6);
                            Map<String,Integer> fn = new HashMap<String, Integer>();
                            for (int i = 0; i < spaceFieldsMap.size(); i++) {
                                Map<String, String> elem = (Map<String, String>) spaceFieldsMap.get(i);
                                fn.put(elem.get("name"), i);
                            }
                            Object fieldsObj = f.get(spaceObject);
                            Field[] spaceFields = fieldsObj.getClass().getFields();
                            for(Field sf:spaceFields) {
                                final String fieldName = fieldsMapping.value().isEmpty() ? sf.getName() : fieldsMapping.value();
                                Integer idx = fn.get(fieldName);
                                if(idx == null) {
                                    throw new IllegalStateException("Can't find field id " + spaceName + "." + fieldName);
                                }
                                sf.set(fieldsObj, sf.getClass().isPrimitive() ? idx.intValue() : idx);
                            }
                        } else if (schemaId != null) {
                            Long value = ((TarantoolConnectionSchemaAware) ops).getSchemaId();
                            if (value == null) {
                                throw new IllegalStateException("Didn't get schema id from server, please check tarantool version");
                            }
                            f.set(spaceObject, f.getClass().isPrimitive() ? value.longValue() : value);
                        }
                    }
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("All schema field should be accessible", e);
                }
            }
        }
        return schema;
    }

    private Map<String, List> callMap(TarantoolConnection16Ops<Integer,Object,Object,List> ops,Integer space, int[] key,String keySeparator) {
        final List<List> tuples = ops.select(space, 0, Arrays.asList(), 0, 1000, 0);
        Map result = new HashMap();
        for (List tuple : tuples) {
            StringBuilder keyValue = new StringBuilder();
            for (Integer k : key) {
                if (keyValue.length() > 0) {
                    keyValue.append(keySeparator);
                }
                keyValue.append(tuple.get(k));
            }
            result.put(keyValue.toString(), tuple);
        }
        return result;
    }
}
