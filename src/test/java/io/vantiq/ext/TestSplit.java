package io.vantiq.ext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import org.junit.Test;

import java.util.*;

public class TestSplit {

    @Test
    public void testSplit() {
        String line = "0.0000||||||||||0.9700";
        line = line.replace("||", ",");
        String[] result = line.split(",");
        System.out.println("result count:" + result.length);

        System.out.println("result:" + Arrays.toString(result));
    }


    @Test
    public void testSplit2() throws JsonProcessingException {
        String line = "0.0000||||||||||0.9700";
        line = line.replace("||", ",");

        CsvMapper mapper = new CsvMapper();
//        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        ObjectReader reader = mapper.readerFor(String[].class);

        String[] result = reader.readValue(line);


        System.out.println("result count:" + result.length);
        System.out.println("result:" + Arrays.toString(result));
    }

    @Test
    public void test3() throws JsonProcessingException {
        String line  = "511182490||2020-03-15 23:00:00||0.0000||0.0000||0.0000||0.0000||0.0000||0.0000||0.0000||0.0000||||||||||0.9700||0.0000||0.0000||0.0000";
        line = line.replace("||", ",");

        CsvMapper csvmapper = new CsvMapper();
//        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        csvmapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        ObjectReader reader = csvmapper.readerFor(String[].class);

        String[] result = reader.readValue(line);
        Map data = new HashMap();
//        data.put("data", Arrays.asList(result));
        List dataList = new ArrayList(3);
        dataList.add(result[0]);
        dataList.add(result[1]);
        dataList.add(result[2]);

        data.put("data", dataList);


        Map<String,Object> m = new LinkedHashMap<>();
        m.put("op", ExtensionServiceMessage.OP_NOTIFICATION);
        m.put("resourceId", "sftp_connector");
        m.put("resourceName", ExtensionServiceMessage.RESOURCE_NAME_SOURCES);
        m.put("object", data);
        ExtensionServiceMessage msg = new ExtensionServiceMessage("");
        msg.fromMap(m);
        ObjectMapper mapper = new ObjectMapper();
        String resultString = mapper.writeValueAsString(msg);


        System.out.println(resultString);
    }

}
