from .basetest import ProjectTest


class SimpleTest(ProjectTest):

    def script_filename(self):
        return 'empty_script.sql'

    def test_database_connection(self):
        cur = self.test_conn.cursor()

        # Run user script
        self.run_user_script()

        # Test results of script
        cur.execute('SELECT 1;')
        self.assertEqual(cur.fetchone()[0], 1)

    def test_schema_exists(self):

        cur = self.test_conn.cursor()
        # Insert data as needed
        cur.execute("INSERT INTO ds.test_table (id) VALUES (1);")

        # Run user script
        self.run_user_script()

        # Test results of script
        cur.execute("SELECT count(*) FROM ds.test_table;")
        self.assertEqual(cur.fetchone()[0], 1)



