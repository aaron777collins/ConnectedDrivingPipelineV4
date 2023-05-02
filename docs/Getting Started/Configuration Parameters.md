## Parameters
There's a lot of configuration parameters that can be used to customize the behavior of the application. The following tables lists all the available parameters and their default values for each respective provider.

// info:

#################  CONFIG FOR ALL PROPERTIES IN THE PIPELINE #################################################

        # used for the logger's path
        self._pathprovider = PathProvider(model=LOG_NAME, contexts={
                "Logger.logpath": DEFAULT_LOG_PATH,
        })

        initialGathererModelName = "CreatingConnectedDrivingDataset"
        numSubsectionRows = 100000

        # Properties:
        # DataGatherer.filepath
        # DataGatherer.subsectionpath
        # DataGatherer.splitfilespath
        # DataGatherer.lines_per_file
        self._initialGathererPathProvider = InitialGathererPathProvider(model=initialGathererModelName, contexts={
            "DataGatherer.filepath": lambda model: "data/data.csv",
            "DataGatherer.subsectionpath": lambda model: f"data/classifierdata/subsection/{model}/subsection{numSubsectionRows}.csv",
            "DataGatherer.splitfilespath": lambda model: f"data/classifierdata/splitfiles/{model}/",
        }
        )

        # Properties:
        #
        # ConnectedDrivingLargeDataCleaner.cleanedfilespath
        # ConnectedDrivingLargeDataCleaner.combinedcleandatapath
        # MConnectedDrivingLargeDataCleaner.dtypes # AUTO_FILLED
        #
        # MAKE SURE TO CHANGE THE MODEL NAME TO THE PROPER NAME (IE A NAME THAT MATCHES IF
        # IT HAS TIMESTAMPS OR NOT, AND IF IT HAS XY COORDS OR NOT, ETC)
        self._generatorPathProvider = GeneratorPathProvider(model=f"{initialGathererModelName}-CCDDWithTimestampsAndWithXYCoords-500mdist", contexts={
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": lambda model:  f"data/classifierdata/splitfiles/cleaned/{model}/",
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": lambda model: f"data/classifierdata/splitfiles/combinedcleaned/{model}/combinedcleaned",
        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.cleandatapath
        # MDataClassifier.plot_confusion_matrix_path
        #
        self._mlPathProvider = MLPathProvider(model=LOG_NAME, contexts={
            "MConnectedDrivingDataCleaner.cleandatapath": lambda model: f"data/mclassifierdata/cleaned/{model}/clean.csv",
            "MDataClassifier.plot_confusion_matrix_path": lambda model: f"data/mclassifierdata/results/{model}/",
        }
        )

        # Properties:
        #
        # DataGatherer.numrows
        # ConnectedDrivingCleaner.x_pos
        # ConnectedDrivingCleaner.y_pos
        # ConnectedDrivingLargeDataCleaner.max_dist
        # ConnectedDrivingLargeDataCleaner.cleanFunc
        # ConnectedDrivingLargeDataCleaner.filterFunc
        # ConnectedDrivingAttacker.SEED
        # ConnectedDrivingCleaner.isXYCoords
        # ConnectedDrivingAttacker.attack_ratio
        # ConnectedDrivingCleaner.cleanFuncName
        #

        # XY columns are added after these columns are used for filtering
        COLUMNS=["metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_accelset_accelYaw","coreData_speed", "coreData_heading", "coreData_position"]


        self.generatorContextProvider = GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numSubsectionRows,
            "DataGatherer.lines_per_file": 1000000,
            "ConnectedDrivingCleaner.x_pos": -105.1159611,
            "ConnectedDrivingCleaner.y_pos": 41.0982327,
            "ConnectedDrivingCleaner.columns": COLUMNS,
            "ConnectedDrivingLargeDataCleaner.max_dist": 500,
            "ConnectedDrivingCleaner.shouldGatherAutomatically": False,
            "ConnectedDrivingLargeDataCleaner.cleanerClass": CleanWithTimestamps,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": CleanWithTimestamps.clean_data_with_timestamps,
            "ConnectedDrivingLargeDataCleaner.cleanerWithFilterClass": CleanerWithFilterWithinRangeXY,
            "ConnectedDrivingLargeDataCleaner.filterFunc": CleanerWithFilterWithinRangeXY.within_rangeXY,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingCleaner.isXYCoords": True,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.cleanFuncName": "clean_data_with_timestamps", # makes cached data have info on if/if not we use timestamps for uniqueness

        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.columns
        # MClassifierPipeline.classifier_instances # AUTO_FILLED
        #
        self.MLContextProvider = MLContextProvider(contexts={
            "MConnectedDrivingDataCleaner.columns": [
            # "metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            #  "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            #  "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            "coreData_id",  # "coreData_position_lat", "coreData_position_long",
            "coreData_secMark", "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "month", "day", "year", "hour", "minute", "second", "pm",
            "coreData_elevation", "coreData_accelset_accelYaw", "coreData_speed", "coreData_heading", "x_pos", "y_pos", "isAttacker"],

            # "MClassifierPipeline.classifier_instances": [...] # AUTO_FILLED

        }
        )

        ######### END OF CONFIG FOR ALL PROPERTIES IN THE PIPELINE ##################################################


#

### PathProvider

Parameter:
`Logger.logpath`

Recommended Value:
`DEFAULT_LOG_PATH`

Get the default log path by importing it from the Logger module

``` python
from Logger.Logger import DEFAULT_LOG_PATH
```

### InitialGathererPathProvider

Parameter:
`DataGatherer.filepath`

Recommended Value:
``` python
lambda model: "data/data.csv"
```

Parameter:
`DataGatherer.subsectionpath`

Recommended Value:
``` python
lambda model: f"data/classifierdata/subsection/{model}/subsection{numSubsectionRows}.csv"
```

Parameter:
`DataGatherer.splitfilespath`

Recommended Value:
``` python
lambda model: f"data/classifierdata/splitfiles/{model}/"
```
