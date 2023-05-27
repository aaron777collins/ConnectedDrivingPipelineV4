**Head to [Setup](./setup.html) if you haven't already setup the project.**

## Creating A Pipeline User File
### File Creation
Pipeline user files should **always** have a unique name that specifies what the file will execute and the parameters that make it unique.

For example, look at the pipeline user file called `MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200`. It runs the large pipeline with the following settings:

- WithXYOffsetPos: Tells us that it will use the XY coordinate system
- 500mDist: This reminds us that we are using the 500m distance filter
- 100kRows: This specifies that we are only using 100k rows to train our data
- EXTTimestampscols: This reminds us that we are cleaning our data to use the extended timestamp columns (which I've included in the file)
- 30attackers: Specifies that we are using 30% attackers (and by default, it is splitting the cars such that 30% are attackers and then making 100% of the BSMs malicious)
- RandOffset100To200: Specifies that we are using the random offset attack with a min distance of 100m and a max distance of 200m.

Pipeline user files should be placed in the base directory of the project. For example, the `MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200` file is placed in the base directory of the project.

### Providers
Providers act as a way to configure the pipeline and set properties that can be accessed within any file. They are implemented as singletons so that they can be instantiated anywhere to access the properties.

The most basic provider is the `DictProvider` and it provides a dictionary of key value pairs that can be accessed by the pipeline. For example, the following provider provides a dictionary with the key `test` and the value `testvalue`:

```python linenums="1"
from ServiceProviders.DictProvider import DictProvider

provider = DictProvider(contexts={"test": "testvalue"})
```

which can be accessed by the pipeline like so:

```python linenums="1"

from ServiceProviders.DictProvider import DictProvider

provider = DictProvider()
testVal = provider.get("test")
print(testVal) # prints "testvalue"
```

However, since the DictProvider is a singleton, we run into trouble if we want to have multiple providers with different values for the same key. Thus, we created context providers for different parts of the pipeline. The ones we used are `GeneratorContextProvider` and `MLContextProvider` which are used for the generator and machine learning parts of the pipeline respectively. These providers are instantiated with a dictionary of key value pairs (similar to DictProvider). For example, the following provider provides a dictionary with the key `test` and the value `testvalue`:

```python linenums="1"
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.MLContextProvider import MLContextProvider

generatorProvider = GeneratorContextProvider(contexts={"test": "testvalue"})
mlProvider = MLContextProvider(contexts={"test": "testvalue2"})
```

which can be accessed by the pipeline like so:

```python linenums="1"
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.MLContextProvider import MLContextProvider

generatorProvider = GeneratorContextProvider()
mlProvider = MLContextProvider()
testVal = generatorProvider.get("test")
testVal2 = mlProvider.get("test")
print(testVal) # prints "testvalue"
print(testVal2) # prints "testvalue2"
```

ContextProviders are useful but we also ran into another issue with paths. We needed the paths for some parts of the pipeline to be unique and other parts to be shared. Thus, we created a variant of the `DictProvider` called the `PathProvider` that uses a model attribute to create a unique element of each value. The user provides a key and value with the key being a string and the value as a function. The function takes in a model name and returns the value. For example, the following `PathProvider` provides a dictionary with the key `test` and the value `{model}/testvalue` where model=`somemodel`:

```python linenums="1"
from ServiceProviders.PathProvider import PathProvider

provider = PathProvider(model="somemodel" contexts={"test": lambda model: f"{model}/testvalue"})
```

which can be accessed by the pipeline like so:

```python linenums="1"
from ServiceProviders.PathProvider import PathProvider

provider = PathProvider(model="somemodel")
testVal = provider.get("test")
print(testVal) # prints "somemodel/testvalue"
```

However, we ran into the same issue with PathProvider, needing a unique path for the initial gatherer, generator, and machine learning parts of the pipeline. Thus, we created the `InitialGathererPathProvider`, `GeneratorPathProvider`, and `MLPathProvider` which are used for the initial gatherer, generator, and machine learning parts of the pipeline respectively. These providers are instantiated with a dictionary of key value pairs (similar to PathProvider). The model name is used to create a unique key for each key value pair. For example, the following provider provides a dictionary with the key `test` and the value `{model}/testvalue` where model=`initialGatherer`, `generator`, or `ml` depending on the provider:

```python linenums="1"
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.MLPathProvider import MLPathProvider

initialGathererProvider = InitialGathererPathProvider(model="initialGatherer", contexts={"test": lambda model: f"{model}/testvalue"})
generatorProvider = GeneratorPathProvider(model="generator", contexts={"test": lambda model: f"{model}/testvalue"})
mlProvider = MLPathProvider(model="ml", contexts={"test": lambda model: f"{model}/testvalue"})
```

which can be accessed by the pipeline like so:

```python linenums="1"
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.MLPathProvider import MLPathProvider

initialGathererProvider = InitialGathererPathProvider(model="initialGatherer")
generatorProvider = GeneratorPathProvider(model="generator")
mlProvider = MLPathProvider(model="ml")

# prints "initialGatherer/testvalue"
initialGathererTestPath = initialGathererProvider.get("test")

# prints "generator/testvalue"
generatorTestPath = generatorProvider.get("test")

# prints "ml/testvalue"
mlTestPath = mlProvider.get("test")
```

### Configuration

## Setting Up The Configuration
All pipeline user files should use a unique class name and log name along with configuration for the providers. The providers can provide a value when requested from a given key. See the [Providers](#providers) section for more information on providers. You can also reference the [Configuration Parameters](./Configuration%20Parameters.html) page to see all the possible configuration parameters.

The following is the config for `MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200`:

```python linenums="1"
# MClassifierPipeline-Const-50-offset

import os
from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from EasyMLLib.CSVWriter import CSVWriter
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Cleaners.CleanersWithFilters.CleanerWithFilterWithinRangeXY import CleanerWithFilterWithinRangeXY
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Cleaners.ConnectedDrivingLargeDataCleaner import ConnectedDrivingLargeDataCleaner
from Generator.Cleaners.ExtraCleaningFunctions.CleanWithTimestamps import CleanWithTimestamps

from Logger.Logger import DEFAULT_LOG_PATH, Logger
from Generator.Cleaners.ConnectedDrivingLargeDataPipelineGathererAndCleaner import ConnectedDrivingLargeDataPipelineGathererAndCleaner
from MachineLearning.MClassifierPipeline import MClassifierPipeline
from MachineLearning.MConnectedDrivingDataCleaner import MConnectedDrivingDataCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider


CLASSIFIER_INSTANCES = [RandomForestClassifier(
), DecisionTreeClassifier(), KNeighborsClassifier()]

LOG_NAME = "MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200"

CSV_COLUMNS = ["Model", "Total_Train_Time",
               "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
               "train_accuracy", "train_precision", "train_recall", "train_f1",
               "test_accuracy", "test_precision", "test_recall", "test_f1"]

CSV_FORMAT = {CSV_COLUMNS[i]: i for i in range(len(CSV_COLUMNS))}


class MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200:

    def __init__(self):

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
        # ConnectedDrivingCleaner.cleanParams
        #

        # Cleaned columns are added/modified after these columns are used for filtering
        COLUMNS=["metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
            "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
            "metadata_serialId_serialNumber", "metadata_receivedAt",
            #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
            #  "metadata_rmd_rxSource","metadata_bsmSource",
            "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
            "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
            "coreData_elevation", "coreData_accelset_accelYaw","coreData_speed", "coreData_heading", "coreData_position"]


        x_pos = -105.1159611
        y_pos = 41.0982327
        x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
        y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
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
            "ConnectedDrivingCleaner.cleanParams": f"clean_data_with_timestamps-within_rangeXY-WithXYCoords-1000mdist-x{x_pos_str}y{y_pos_str}dd02mm04yyyy2021", # makes cached data have info on if/if not we use timestamps for uniqueness

        }
        )

        # Properties:
        #
        # MConnectedDrivingDataCleaner.columns
        # MClassifierPipeline.classifier_instances # AUTO_FILLED
        # MClassifierPipeline.csvWriter
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
            "MClassifierPipeline.csvWriter": CSVWriter(f"{LOG_NAME}.csv", CSV_COLUMNS),

        }
        )

        ######### END OF CONFIG FOR ALL PROPERTIES IN THE PIPELINE ##################################################

        self.logger = Logger(LOG_NAME)
        self.csvWriter = CSVWriter(f"{LOG_NAME}.csv", CSV_COLUMNS)

    def write_entire_row(self, dict):
        row = [" "]*len(CSV_COLUMNS)
        # Writing each variable to the row
        for d in dict:
            row[CSV_FORMAT[d]] = dict[d]

        self.csvWriter.addRow(row)
```

#### Classifier Models

You'll notice that we default to using the RandomForestClassifier, DecisionTreeClassifier and KNeighborsClassifier models for the MClassifierPipeline. This is because these models are the most accurate for our data. However, you can change this to use any model you want. You can also change the parameters for each model. For example, if you want to use a different number of estimators for the RandomForestClassifier, you can change the parameters to be:

```python linenums="1"
CLASSIFIER_INSTANCES = [
    RandomForestClassifier(n_estimators=200), # 100 is default
    DecisionTreeClassifier(),
    KNeighborsClassifier()
]
```

**However, make sure to include the `classifier_instances` property in the MLContextProvider and set it to `CLASSIFIER_INSTANCES`**

#### Log Name
The log name is **very important** because it ensures that the caching used in the machine learning part of the pipeline works correctly. It also acts as the log folder name for our logs.

#### CSV Columns
These columns act as our CSV headers for our results file. You won't need to change these unless you plan to modify the way we write our results to the CSV file.

#### CSV Format
This is a dictionary that maps the CSV column names to their index in the CSV file. You won't need to change these unless you plan to modify the way we write our results to the CSV file.

#### PathProvider
You'll notice that the first part we configure within the init method is the PathProvider. We use this to ensure that our logs are written to the correct folder. You won't need to change this unless you plan to modify the way we write our logs. **Make sure that the LOG_NAME is unique to each pipeline user file.**

```python linenums="1"
# used for the logger's path
self._pathprovider = PathProvider(model=LOG_NAME, contexts={
        "Logger.logpath": DEFAULT_LOG_PATH,
})
```

#### InitialGathererPathProvider
The paths in the InitialGathererPathProvider are used to gather the data from the initial data source. You'll need to change these to match the paths to your data. **The model name should be shared among all similar initial datasets (i.e. all datasets that are gathered from the same source).**

```python linenums="1"
initialGathererModelName = "CreatingConnectedDrivingDataset"
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
```

#### GeneratorPathProvider
The paths in the GeneratorPathProvider are used to generate the data for the machine learning part of the pipeline. You'll need to change these to match the paths to your generated data. **Make sure that the model name is unique to the cleaning/filtering options. For example, it should include whether or not we included timestamps, XY COORDS, etc. and also the distance of the filter (default 500m)**

```python linenums="1"
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
```

#### MLPathProvider
The paths in the MLPathProvider are used to run the machine learning part of the pipeline. You'll need to change these to match the paths to your ML data. **Make sure that the model name is unique to the ML options and cleaning options. The best way to do this is to make your file name unique and set the model as the file name (A.K.A. the LOG_NAME)**

```python linenums="1"
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
```

#### COLUMNS

The COLUMNS variable is used to specify the columns that we want to use for our initial cleaning and gathering of data. You'll need to change these to match the columns in your data if they are different.

```python linenums="1"
# Cleaned columns are added/modified after these columns are used for filtering
COLUMNS=["metadata_generatedAt", "metadata_recordType", "metadata_serialId_streamId",
    "metadata_serialId_bundleSize", "metadata_serialId_bundleId", "metadata_serialId_recordId",
    "metadata_serialId_serialNumber", "metadata_receivedAt",
    #  "metadata_rmd_elevation", "metadata_rmd_heading","metadata_rmd_latitude", "metadata_rmd_longitude", "metadata_rmd_speed",
    #  "metadata_rmd_rxSource","metadata_bsmSource",
    "coreData_id", "coreData_secMark", "coreData_position_lat", "coreData_position_long",
    "coreData_accuracy_semiMajor", "coreData_accuracy_semiMinor",
    "coreData_elevation", "coreData_accelset_accelYaw","coreData_speed", "coreData_heading", "coreData_position"]
```

#### GeneratorContextProvider
The GeneratorContextProvider is used to provide the contexts for the generation part of the pipeline (including the initial gathering part). You'll need to change these to match configurations for your data. Make sure to change the cleanParams to match the cleaning options you want to use. **Make sure that the cleanParams is unique to the cleaning/filtering options. For example, it should include whether or not we included timestamps, XY COORDS, etc. and also the distance of the filter.**

```python linenums="1"
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
# ConnectedDrivingCleaner.cleanParams
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


x_pos = -105.1159611
y_pos = 41.0982327
x_pos_str = MathHelper.convertNumToTitleStr(x_pos)
y_pos_str = MathHelper.convertNumToTitleStr(y_pos)
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
    "ConnectedDrivingCleaner.cleanParams": f"clean_data_with_timestamps-within_rangeXY-WithXYCoords-1000mdist-x{x_pos_str}y{y_pos_str}dd02mm04yyyy2021", # makes cached data have info on if/if not we use timestamps for uniqueness

}
)
```

#### MLContextProvider
The MLContextProvider is used to provide the contexts for the machine learning part of the pipeline. You'll need to change these to match configurations for your ML data.

```python linenums="1"
# Properties:
#
# MConnectedDrivingDataCleaner.columns
# MClassifierPipeline.classifier_instances # AUTO_FILLED
# MClassifierPipeline.csvWriter
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
    "MClassifierPipeline.csvWriter": CSVWriter(f"{LOG_NAME}.csv", CSV_COLUMNS),

}
)
```

#### Logger and CSV Writer
The logger and CSV writer are used to log the results of the pipeline. You won't need to change these.

```python linenums="1"
# Goes after the config ...

self.logger = Logger(LOG_NAME)
self.csvWriter = self.MLContextProvider.get("MClassifierPipeline.csvWriter")
```

#### write_entire_row function
The write_entire_row function is used to write the results of the pipeline to a CSV file. You won't need to change this.

```python linenums="1"
def write_entire_row(self, dict):
    row = [" "]*len(CSV_COLUMNS)
    # Writing each variable to the row
    for d in dict:
        row[CSV_FORMAT[d]] = dict[d]

    self.csvWriter.addRow(row)
```

### Creating The `run` Function

The `run` function is the main function of the pipeline. It is called when the pipeline is run. You'll need to change this to match your pipeline.

#### An Example `run` Function
The following is the run function for `MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200`

```python linenums="1"
def run(self):

    mcdldpgac = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

    data: DataFrame = mcdldpgac.getNRows(200000)

    # splitting into train and test sets
    train = data.iloc[:100000].copy()
    test = data.iloc[100000:200000].copy()

    # Note you can randomize the sampling as follows:
    # # splitting into train and test sets
    # seed = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED")
    # train, test = train_test_split(data, test_size=0.2, random_state=seed)

    # cleaning/adding attackers to the data
    train = StandardPositionalOffsetAttacker(train, "train").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()
    test = StandardPositionalOffsetAttacker(test, "test").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()



    # Cleaning it for the malicious data detection
    mdcleaner_train = MConnectedDrivingDataCleaner(train, "train")
    mdcleaner_test = MConnectedDrivingDataCleaner(test, "test")
    m_train = mdcleaner_train.clean_data().get_cleaned_data()
    m_test = mdcleaner_test.clean_data().get_cleaned_data()

    # splitting into features and the labels
    attacker_col_name = "isAttacker"
    train_X = m_train.drop(columns=[attacker_col_name], axis=1)
    train_Y = m_train[attacker_col_name]
    test_X = m_test.drop(columns=[attacker_col_name], axis=1)
    test_Y = m_test[attacker_col_name]

    # training the classifiers
    mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y)

    mcp.train()
    mcp.test()

    # getting the results
    results = mcp.calc_classifier_results().get_classifier_results()

    # printing the results
    for mclassifier, train_result, result in results:
        mcp.logger.log(mclassifier)
        mcp.logger.log("Train Set Results:")
        mcp.logger.log("Accuracy: ", train_result[0])
        mcp.logger.log("Precision: ", train_result[1])
        mcp.logger.log("Recall: ", train_result[2])
        mcp.logger.log("F1: ", train_result[3])
        mcp.logger.log("Test Set Results:")
        mcp.logger.log("Accuracy: ", result[0])
        mcp.logger.log("Precision: ", result[1])
        mcp.logger.log("Recall: ", result[2])
        mcp.logger.log("F1: ", result[3])
        # printing the elapsed training and prediction time
        mcp.logger.log("Elapsed Training Time: ",
                        mclassifier.elapsed_train_time)
        mcp.logger.log("Elapsed Prediction Time: ",
                        mclassifier.elapsed_prediction_time)

        mcp.logger.log("Writing to CSV...")

        # writing entire row to csv
        # columns: "Model", "Total_Train_Time",
        #    "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
        #    "train_accuracy", "train_precision", "train_recall", "train_f1",
        #    "test_accuracy", "test_precision", "test_recall", "test_f1"

        csvrowdata = {
            "Model": mclassifier.classifier.__class__.__name__,
            "Total_Train_Time": mclassifier.elapsed_train_time,
            # train and test have the same number of samples
            "Total_Train_Sample_Size": len(train_X),
            # train and test have the same number of samples
            "Total_Test_Sample_Size": len(test_X),
            "Train_Time_Per_Sample": mclassifier.elapsed_train_time/len(train_X),
            "Prediction_Train_Set_Time_Per_Sample": mclassifier.elapsed_prediction_train_time/len(train_X),
            "Prediction_Test_Set_Time_Per_Sample": mclassifier.elapsed_prediction_time/len(test_X),
            "train_accuracy": train_result[0],
            "train_precision": train_result[1],
            "train_recall": train_result[2],
            "train_f1": train_result[3],
            "test_accuracy": result[0],
            "test_precision": result[1],
            "test_recall": result[2],
            "test_f1": result[3]}
        self.write_entire_row(csvrowdata)

    # calculating confusion matrices and storing them
    mcp.logger.log("Calculating confusion matrices and storing...")
    mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices()
```

#### Explaining How The `run` Function Works
The `run` function acts as your main method for your pipeline.

1. You'll notice that this pipeline uses the `ConnectedDrivingLargeDataPipelineGathererAndCleaner` class. It acts as a gatherer and cleaner for the data. It is used to split the data from the CSV files, clean them, combine them into one DataFrame. We can call the `run` function of the `ConnectedDrivingLargeDataPipelineGathererAndCleaner` class to get the data. We can then use the `getNRows` function to get the first 200 000 rows of the data.

```python linenums="1"
mcdldpgac = ConnectedDrivingLargeDataPipelineGathererAndCleaner().run()

data: DataFrame = mcdldpgac.getNRows(200000)
```

2. We then split the data into a train and test set.

```python linenums="1"
# splitting into train and test sets
train = data.iloc[:100000].copy()
test = data.iloc[100000:200000].copy()
# Note you could also randomize the sampling as follows:
seed = self.generatorContextProvider.get("ConnectedDrivingAttacker.SEED")
train, test = train_test_split(data, test_size=0.2, random_state=seed)
```

3. Adding attackers to the data.

```python linenums="1"
# cleaning/adding attackers to the data
train = StandardPositionalOffsetAttacker(train, "train").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()
test = StandardPositionalOffsetAttacker(test, "test").add_attackers().add_attacks_positional_offset_rand(min_dist=100, max_dist=200).get_data()
```

4. Cleaning the data using the `MConnectedDrivingDataCleaner` class.

``` python linenums="1"
 # Cleaning it for the malicious data detection
mdcleaner_train = MConnectedDrivingDataCleaner(train, "train")
mdcleaner_test = MConnectedDrivingDataCleaner(test, "test")
m_train = mdcleaner_train.clean_data().get_cleaned_data()
m_test = mdcleaner_test.clean_data().get_cleaned_data()
```

5. Splitting the data into features and labels.

```python linenums="1"
attacker_col_name = "isAttacker"
train_X = m_train.drop(columns=[attacker_col_name], axis=1)
train_Y = m_train[attacker_col_name]
test_X = m_test.drop(columns=[attacker_col_name], axis=1)
test_Y = m_test[attacker_col_name]
```

6. Training the classifiers using the `MClassifierPipeline` class.

```python linenums="1"
mcp = MClassifierPipeline(train_X, train_Y, test_X, test_Y)

mcp.train()
mcp.test()
```

7. Getting the results of the classifiers.

```python linenums="1"
results = mcp.calc_classifier_results().get_classifier_results()
```

8. Printing the results of the classifiers.

```python linenums="1"
# printing the results
for mclassifier, train_result, result in results:
    mcp.logger.log(mclassifier)
    mcp.logger.log("Train Set Results:")
    mcp.logger.log("Accuracy: ", train_result[0])
    mcp.logger.log("Precision: ", train_result[1])
    mcp.logger.log("Recall: ", train_result[2])
    mcp.logger.log("F1: ", train_result[3])
    mcp.logger.log("Test Set Results:")
    mcp.logger.log("Accuracy: ", result[0])
    mcp.logger.log("Precision: ", result[1])
    mcp.logger.log("Recall: ", result[2])
    mcp.logger.log("F1: ", result[3])
    # printing the elapsed training and prediction time
    mcp.logger.log("Elapsed Training Time: ",
                    mclassifier.elapsed_train_time)
    mcp.logger.log("Elapsed Prediction Time: ",
                    mclassifier.elapsed_prediction_time)

    mcp.logger.log("Writing to CSV...")

    # writing entire row to csv
    # columns: "Model", "Total_Train_Time",
    #    "Total_Train_Sample_Size", "Total_Test_Sample_Size", "Train_Time_Per_Sample", "Prediction_Train_Set_Time_Per_Sample", "Prediction_Test_Set_Time_Per_Sample",
    #    "train_accuracy", "train_precision", "train_recall", "train_f1",
    #    "test_accuracy", "test_precision", "test_recall", "test_f1"

    csvrowdata = {
        "Model": mclassifier.classifier.__class__.__name__,
        "Total_Train_Time": mclassifier.elapsed_train_time,
        # train and test have the same number of samples
        "Total_Train_Sample_Size": len(train_X),
        # train and test have the same number of samples
        "Total_Test_Sample_Size": len(test_X),
        "Train_Time_Per_Sample": mclassifier.elapsed_train_time/len(train_X),
        "Prediction_Train_Set_Time_Per_Sample": mclassifier.elapsed_prediction_train_time/len(train_X),
        "Prediction_Test_Set_Time_Per_Sample": mclassifier.elapsed_prediction_time/len(test_X),
        "train_accuracy": train_result[0],
        "train_precision": train_result[1],
        "train_recall": train_result[2],
        "train_f1": train_result[3],
        "test_accuracy": result[0],
        "test_precision": result[1],
        "test_recall": result[2],
        "test_f1": result[3]}
    self.write_entire_row(csvrowdata)
```

9. Calculating the confusion matrices and storing them. *Note that this step also saved the matrices in your results file as base64 images.*

```python linenums="1"
# calculating the confusion matrices
mcp.calculate_classifiers_and_confusion_matrices().plot_confusion_matrices()
```

### Running The Pipeline
To run the pipeline, you can run the following command in the terminal:

```bash
python <your python file>.py
```
If you haven't already, make sure to activate your virtual environment before running the command.
See the [Setup](./setup.html) page for more details.

### Running On The Super Computer
To run on the super computer, register with compute canada.
Next, follow the instructions on the [Setup](./setup.html) page to set up your virtual environment. You'll likely
want to use the beluga super computer (i.e. <username>@beluga.computecanada.ca)
You may want to use --no-index when installing requirements to avoid downloading packages from the internet.

Finally, you can run the following command to submit a job to the super computer:

```bash
./runUserPipeline.sh <USERNAME> <PATH_TO_REPO (not ending in slash)> <FILE> <DAYS> <HOURS> <MINUTES> <CPUS> <MEM> [OPTIONAL: DEPENDENCY]
```

Optionally, you can use the defaultrunnerconfig.sh file to use default values:

```bash
./defaultrunnerconfig.sh <FILE> <USERNAME> [OPTIONAL: DEPENDENCY]
```

If you decide to run the commands directly from the super computer, you can generate a slurm file
using the following command:

```bash
python3 generateSlurm.py <fileName> <days> <hours> <minutes> <cpus> <memory(G)> [dependency]
```

Or you can create the slurm file yourself and submit it as a job using the following command:

```bash
sbatch <fileName>
```
