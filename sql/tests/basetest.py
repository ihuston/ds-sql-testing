from unittest import TestCase
import uuid
import psycopg2 as pg
from psycopg2.extensions import AsIs, ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor
import os
import subprocess

# Get connection credentials from environment
host = os.getenv('DS_TEST_HOST')
user = os.getenv("DS_TEST_USER")
password = os.getenv("DS_TEST_PASSWORD")


class ProjectTest(TestCase):

    def setUp(self):
        self.admin_conn = pg.connect(user=user, password=password, host=host,
                                     database=user)
        self.test_db = 'test_' + str(uuid.uuid4()).replace("-", "_")
        self.drop_database()
        self.create_database()
        self.load_schema()
        self.test_conn = pg.connect(user=user, password=password, host=host,
                                    database=self.test_db, cursor_factory=DictCursor)

    def create_database(self):
        self.admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = self.admin_conn.cursor()
        cur.execute('CREATE DATABASE %s;', (AsIs(self.test_db),))
        cur.close()

    def load_schema(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(test_dir, './load_schema.sql')
        self.execute_script(schema_path)

    def run_user_script(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        sql_dir = os.path.dirname(test_dir)
        script_path = os.path.join(sql_dir, self.script_filename())
        return self.execute_script(script_path)

    def execute_script(self, script_path):
        curdir = os.curdir
        os.chdir(os.path.dirname(script_path))
        subprocess.check_call(["psql", "-q", "-h", host, "-U", user,
                               "-f", script_path, self.test_db])
        os.chdir(curdir)
        return

    def script_filename(self):
        raise ValueError('Please provide a SQL script filename!')

    def drop_database(self):
        self.admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = self.admin_conn.cursor()
        try:
            cur.execute('SELECT 1;')
            cur.execute('DROP DATABASE IF EXISTS %s;', (AsIs(self.test_db),))
        except pg.OperationalError as e:
            print('Not able to drop database {}'.format(self.test_db))
            print('Error: {}'.format(e))
            # Who is accessing database?
            cur.execute("SELECT * FROM pg_stat_activity where datname=%s;", (self.test_db,))
            res = cur.fetchall()
            print("pg_stat_activity: " + str(res))
            # Try again
            cur.execute("SELECT 1;")
            cur.execute("DROP DATABASE IF EXISTS %s;", (AsIs(self.test_db),))
            print("Second attempt to drop database succeeded.")
        else:
            # print('Dropped database {}'.format(self.test_db))
            pass
        cur.close()

    def tearDown(self):
        self.test_conn.close()
        self.drop_database()




