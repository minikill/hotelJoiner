package inc.sad.stage.processor.hotelJoiner;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import inc.sad.stage.lib.hotelJoiner.Errors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class HotelJoinerProcessor extends SingleLaneRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(HotelJoinerProcessor.class);

    /**
     * Gives access for connection properties from UI
     * @return
     */
    public abstract String getHdfsURL();
    public abstract String getHdfsPath();

    /**
     * Data store of hotels
     */
    private Map<Long, String> hotelsMap = new HashMap<>();
    private String[] headers;

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        initHotelsMap(issues);
        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        super.destroy();
    }

    /**
     * {@inheritDoc}
     * Processing each record for joining expedia and hotels data
     */
    @Override
    protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        try {
            Field hotelIdField = record.get("/hotel_id");
            Long hotelId = hotelIdField.getValueAsLong();

            String hotelData = hotelsMap.get(hotelId);

            if (hotelData == null) {
                LOG.error("Record with not matched id: {}", record.get("/hotel_id"));
                return;
            }

            String[] hotelInfoValues = hotelData.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            for (int i = 0; i < hotelInfoValues.length; i++) {
                record.set( "/hotel_" + headers[i].toLowerCase(), Field.create(hotelInfoValues[i]));
            }

            batchMaker.addRecord(record);
            LOG.error("Record added id: {}", record.get("/hotel_id"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method for initialization dataStore
     * @param issues
     * @return initialized map of hotels data
     */
    private void initHotelsMap(List<ConfigIssue> issues) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", getHdfsURL());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        LOG.info("Initialization started!");
        try (FileSystem fs = FileSystem.get(conf)) {
            Path path = new Path(getHdfsPath());
            LOG.info(String.valueOf(path.toUri()));
            LOG.info(String.valueOf(fs.exists(path)));
            if (fs.exists(path)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    headers = reader.readLine().split(",");
                    reader.lines().forEach(line -> {
                        Long hotelId = Long.parseLong(line.split(",")[0]);
                        LOG.info(hotelsMap.put(hotelId, line));
                    });
                }
            }
        }catch (IOException e) {
            LOG.error("Some problem while initialization", e);
            ConfigIssue configIssue = getContext()
                    .createConfigIssue(Groups.HOTELS_DATA.name(), "config", Errors.HOTELS_00, e.getMessage());
            issues.add(configIssue);
        }
    }
}