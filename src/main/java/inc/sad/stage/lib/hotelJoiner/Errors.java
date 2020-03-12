package inc.sad.stage.lib.hotelJoiner;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

/**
 * Enum for handling errors inside of processor
 */
@GenerateResourceBundle
public enum Errors implements ErrorCode {

    HOTELS_00("We are in trouble");
    private final String msg;

    Errors(String msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override
    public String getCode() {
        return name();
    }

    /** {@inheritDoc} */
    @Override
    public String getMessage() {
        return msg;
    }
}
