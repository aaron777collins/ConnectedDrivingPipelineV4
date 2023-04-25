class DataConverter:
    @staticmethod
    # Point is a string in the format POINT (-104.6744332 41.1509182)
    def point_to_tuple(point: str)->tuple[float,float]:
        point = point.replace('POINT (', '')
        point = point.replace(')', '')
        point = point.split(' ')
        return [float(point[0]), float(point[1])]
