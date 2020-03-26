package io.vantiq.ext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.junit.Test;

import java.util.Arrays;

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

}
