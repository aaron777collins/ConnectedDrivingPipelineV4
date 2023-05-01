**Head to [Setup](./setup.html) if you haven't already setup the project.**

## Creating A Pipeline User File

Pipeline user files should **always** have a unique name that specifies what the file will execute and the parameters that make it unique.

For example, look at the pipeline user file called `MClassifierLargePipelineUserWithXYOffsetPos500mDist100kRowsEXTTimestampsCols30attackersRandOffset100To200`. It runs the large pipeline with the following settings:

- WithXYOffsetPos: Tells us that it will use the XY coordinate system
- 500mDist: This reminds us that we are using the 500m distance filter
- 100kRows: This specifies that we are only using 100k rows to train our data
- EXTTimestampscols: This reminds us that we are cleaning our data to use the extended timestamp columns (which I've included in the file)
- 30attackers: Specifies that we are using 30% attackers (and by default, it is splitting the cars such that 30% are attackers and then making 100% of the BSMs malicious)
- RandOffset100To200: Specifies that we are using the random offset attak with a min distance of 100m and a max distance of 200m.

## Setting Up The Pipeline Config

