import math
from Helpers.MathHelper import MathHelper
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.KeyProvider import KeyProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestLocationDiff(ITest):

    def run(self):
        # pass
        # adding data to the other providers to test overlap
        x1, y1 = -106.0831353, 41.5430216
        x2, y2 = -105.1159611, 41.0982327

        # lat1, long1 = 41.5430216, -106.0831353
        # lat2, long2 = 41.0982327, -105.1159611
        dist = MathHelper.dist_between_two_points(y1, x1, y2, x2)

        print(f"dist between {x1}, {y1} and {x2}, {y2} is {dist}?")

        latDistConverter = 111_111.1 # meters
        longDistConverter = 74_779.2 # meters

        xDiff = (x1-x2) * longDistConverter
        yDiff = (y1-y2) * latDistConverter

        metersDist = math.sqrt((xDiff)**2 + (yDiff)**2)

        print(f"meters dist between {x1}, {y1} and {x2}, {y2} is {metersDist}m (roughly)")





    def cleanup(self):
        pass
