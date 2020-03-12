/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package inc.sad.stage.processor.hotelJoiner;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

/**
 * Class for describing UI part of processor
 */
@StageDef(
        version = 1,
        label = "HotelJoiner Processor",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HotelJoinerDProcessor extends HotelJoinerProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "HDFS Hotels path",
            displayPosition = 10,
            group = "HOTELS_DATA"
    )
    public String hdfsPath;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "HDFS FS URL",
            displayPosition = 10,
            group = "HOTELS_DATA"
    )
    public String hdfsURL;

    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getHdfsURL() {
        return hdfsURL;
    }

}