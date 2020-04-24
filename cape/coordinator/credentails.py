class Credentials:
    def __init__(self, salt, alg):
        self.salt = salt
        self.alg = alg

    def __eq__(self, other):
        return self.salt == other.salt and self.alg == other.alg
