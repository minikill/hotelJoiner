package inc.sad.stage.processor.hotelJoiner;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


@RunWith(MockitoJUnitRunner.class)
class HotelJoinerProcessorTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    void process() throws IOException {
        testFolder.create();
        File file = testFolder.newFile();

        try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
            String stringToWrite = String.join("\n", TestData.HOTELS_HEADERS, TestData.HOTEL_DATA);
            outputStream.write(stringToWrite.getBytes());
        }

        ProcessorRunner runner = new ProcessorRunner.Builder(HotelJoinerDProcessor.class)
                .addConfiguration("hdfsURL", "file://localhost")
                .addConfiguration("hdfsPath", file.getAbsolutePath())
                .addOutputLane("output")
                .build();

        runner.runInit();

        try {
            Record record = RecordCreator.create();
            record.set(Field.create(true));
            StageRunner.Output output = runner.runProcess(Arrays.asList(TestData.TEST_RECORD));
            Record processed = output.getRecords().get("output").get(0);
            Assert.assertEquals("It's OK", TestData.EXPECTED_RECORD, processed);
        } finally {
            runner.runDestroy();
        }

    }

    private static class TestData {
        private static final String HOTELS_HEADERS = "Id,Name,Country,City,Address,Latitude,Longitude";
        private static final String HOTEL_DATA = "85899345924,The Parkview Memphis,US,Memphis,1914 Poplar Ave,35.142477,-89.996546";
        private static final Record TEST_RECORD = createRecord(createFieldMap());
        private static final Record EXPECTED_RECORD = createExpected(HOTEL_DATA, TEST_RECORD);


        private static Record createRecord(Map<String, Field> fieldMap) {
            Record testRecord = RecordCreator.create();
            testRecord.set(Field.createListMap(new LinkedHashMap<>(fieldMap)));
            return testRecord;
        }

        private static Map<String, Field> createFieldMap() {
            Map<String, Field> fieldMap = new HashMap<>();

            fieldMap.put("id", Field.create(1L));
            fieldMap.put("date_time", Field.create("2020-03-13 10:44:10"));
            fieldMap.put("site_name", Field.create(2));
            fieldMap.put("posa_continent", Field.create(3));
            fieldMap.put("user_location_country", Field.create(4));
            fieldMap.put("user_location_region", Field.create(5));
            fieldMap.put("user_location_city", Field.create(6));
            fieldMap.put("orig_destination_distance", Field.create(7.7910));
            fieldMap.put("user_id", Field.create(11));
            fieldMap.put("is_mobile", Field.create(0));
            fieldMap.put("is_package", Field.create(1));
            fieldMap.put("channel", Field.create(14));
            fieldMap.put("srch_ci", Field.create("2020-03-12"));
            fieldMap.put("srch_co", Field.create("2020-03-13"));
            fieldMap.put("srch_adults_cnt", Field.create(15));
            fieldMap.put("srch_children_cnt", Field.create(16));
            fieldMap.put("srch_rm_cnt", Field.create(17));
            fieldMap.put("srch_destination_id", Field.create(18));
            fieldMap.put("srch_destination_type_id", Field.create(19));
            fieldMap.put("hotel_id", Field.create(85899345924L));

            return fieldMap;
        }

        private static Record createExpected(String hotelData, Record expediaRecord) {
            try {
                String[] hotelInfoValues = hotelData.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                for (int i = 0; i < hotelInfoValues.length; i++) {
                    expediaRecord.set( "/hotel_" + HOTELS_HEADERS.split(",")[i].toLowerCase(), Field.create(hotelInfoValues[i]));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return expediaRecord;
        }
    }

}