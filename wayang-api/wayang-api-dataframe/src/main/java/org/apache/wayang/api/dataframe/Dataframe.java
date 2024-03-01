package org.apache.wayang.api.dataframe;

import java.util.HashMap;
import java.util.Map;
import org.apache.wayang.api.DataQuanta;

public class Dataframe {

    public DataQuanta<String> dataquanta;
    private Map<String, Integer> mappings;

    private Map<String, Integer> createMappings(String[] schema) {
        Map<String, Integer> mappings = new HashMap<>();
        for (int i = 0; i < schema.length; i++) {
            mappings.put(schema[i], i);
        }
        return mappings;
    }

    public Dataframe(DataQuanta<String> dataquanta, String[] schema) {
        this.dataquanta = dataquanta;
        this.mappings = createMappings(schema);
    }

}


