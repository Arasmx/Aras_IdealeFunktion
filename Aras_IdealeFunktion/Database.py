class Database:

    def __init__(self, user, password, host, database):
        self.user: user
        self.password: password
        self.host: host
        self.database: database
        self.config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database
        }
