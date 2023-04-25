class IDMapper:
    # creates a SIMPLE singleton (can also be switched for the better singletons in ClassTypes)
    # that maps a given ID to 1, 2, 3, etc.
    id_map = {}
    id_counter = 0
    @classmethod
    def getID(cls, id):
        if id not in cls.id_map:
            cls.id_counter += 1
            cls.id_map[id] = cls.id_counter
        return cls.id_map[id]

