# IMPORTING REQ'D LIBRARIES
import mysql.connector
from tabulate import tabulate
from decimal import Decimal


# FUNCTION TO PRINT SQL TABLE
def print_mysql_table(cnx, table_name):
    # Create cursor
    cursor = cnx.cursor()

    # Execute query to select all data from table
    cursor.execute(f"SELECT * FROM {table_name}")

    # Get column names from cursor description
    column_names = [column[0] for column in cursor.description]

    # Fetch all rows from the table
    rows = cursor.fetchall()

    # Print the table using tabulate
    print(tabulate(rows, headers=column_names, tablefmt="pipe"))


# CLASS TO EMULATE MONGODB USING A MYSQL BACKEND
class EmulatedMongoClient:
    # Connect to the MySQL database
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    # Access a database in the MongoDB emulation
    def __getitem__(self, db_name):
        return EmulatedDatabase(self.connection, db_name)


# CLASS TO REPRESENT A DATABASE IN THE MONGODB EMULATION
class EmulatedDatabase:

    def __init__(self, connection, db_name):
        self.connection = connection
        self.db_name = db_name

    # Access a collection in the database
    def __getitem__(self, collection_name):
        return EmulatedCollection(self.connection, self.db_name, collection_name)

    # Create a collection in the database
    def create_collection(self, collection_name, schema=None):
        # If a schema is provided, create the columns based on the schema
        if schema:
            columns = ', '.join([f"{key} {value}" for key, value in schema.items()])
        else:
            # If no schema is provided, create a default 'id' column
            columns = "id INT AUTO_INCREMENT PRIMARY KEY"

        # Create cursor
        cursor = self.connection.cursor()

        # Drop the table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {collection_name}")

        # Create the table with the specified columns
        cursor.execute(f"CREATE TABLE {collection_name} ({columns})")

        # Commit the changes to the database
        self.connection.commit()


# CLASS TO REPRESENT A COLLECTION IN THE MONGODB EMULATION
class EmulatedCollection:

    def __init__(self, connection, db_name, collection_name):
        self.connection = connection
        self.db_name = db_name
        self.collection_name = collection_name

    # Insert a single document into the collection
    def insert_one(self, document):
        # Get the keys from the document
        keys = ', '.join(document.keys())

        # Get the values from the document
        values = ', '.join([f"'{value}'" for value in document.values()])

        # Create cursor
        cursor = self.connection.cursor()

        # Execute an INSERT statement to insert the document into the collection
        cursor.execute(f"INSERT INTO {self.collection_name} ({keys}) VALUES ({values})")

        # Commit the changes to the database
        self.connection.commit()

    # Update a single document in the collection
    def update_one(self, query, update):
        # Create a 'SET' clause from the '$set' key in the update
        set_clause = ', '.join([f"{key} = '{value}'" for key, value in update['$set'].items()])

        # Create a 'WHERE' clause from the query
        where_clause = ' AND '.join([f"{key} = '{value}'" for key, value in query.items()])

        # Create cursor
        cursor = self.connection.cursor()

        # Execute an UPDATE statement to update the document in the collection
        cursor.execute(f"UPDATE {self.collection_name} SET {set_clause} WHERE {where_clause}")

        # Commit the changes to the database
        self.connection.commit()

    # Find documents in the collection
    def find(self, query=None, projection=None):
        # Create cursor
        cursor = self.connection.cursor()

        # Define a helper function to process query conditions
        def process_conditions(query):
            conditions = []
            for key, value in query.items():
                if isinstance(value, list):
                    if key == "$or":
                        or_conditions = []
                        for subquery in value:
                            for k, v in subquery.items():
                                subquery_conditions = process_conditions({k: v})
                                or_conditions.append("(" + subquery_conditions + ")")

                        or_clause = " OR ".join(or_conditions)
                        conditions.append(f"({or_clause})")

                    if key == "$and":
                        and_conditions = []
                        for subquery in value:
                            for k, v in subquery.items():
                                subquery_conditions = process_conditions({k: v})
                                and_conditions.append("(" + subquery_conditions + ")")

                        and_clause = " AND ".join(and_conditions)
                        conditions.append(f"({and_clause})")

                elif isinstance(value, dict):
                    for op, op_value in value.items():
                        if op == "$gt":
                            conditions.append(f"{key} > '{op_value}'")
                        elif op == "$lt":
                            conditions.append(f"{key} < '{op_value}'")
                        elif op == "$gte":
                            conditions.append(f"{key} >= '{op_value}'")
                        elif op == "$lte":
                            conditions.append(f"{key} <= '{op_value}'")
                        else:
                            raise ValueError(f"Unsupported operation '{op}'")
                else:
                    conditions.append(f"{key} = '{value}'")

            return ' AND '.join(conditions)

        # Create a 'SELECT' clause from the projection
        if projection:
            select_fields = ', '.join([f"{key}" for key, value in projection.items() if value == 1])
        else:
            select_fields = '*'

        # Create a 'WHERE' clause from the query
        if query:
            where_clause = process_conditions(query)
            cursor.execute(f"SELECT {select_fields} FROM {self.collection_name} WHERE {where_clause}")
        else:
            cursor.execute(f"SELECT {select_fields} FROM {self.collection_name}")

        # Fetch all rows from the query result
        results = cursor.fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Convert the query result to a list of dictionaries
        return [dict(zip(column_names, result)) for result in results]

    # Perform aggregation on the collection
    def aggregate(self, pipeline):

        # Define a helper function to convert Decimal values to integers to integers
        def decimal_to_int(value):
            if isinstance(value, (Decimal, float)):
                return int(value)
            return value

        # Process conditions
        def process_conditions(conditions):
            where_clause_parts = []
            for key, value in conditions.items():
                if isinstance(value, dict):
                    for op, op_value in value.items():
                        if op == "$gt":
                            where_clause_parts.append(f"{key} > {op_value}")
                        elif op == "$lt":
                            where_clause_parts.append(f"{key} < {op_value}")
                        else:
                            raise ValueError(f"Unsupported operation '{op}'")
                else:
                    where_clause_parts.append(f"{key} = '{value}'")
            return ' AND '.join(where_clause_parts)

        # Create cursor
        cursor = self.connection.cursor()

        # Build the query string from the pipeline stages
        query = f"SELECT * FROM {self.collection_name}"

        for stage in pipeline:
            stage_name = list(stage.keys())[0]
            stage_value = stage[stage_name]

            # Process a '$match' stage
            if stage_name == "$match":
                where_clause = process_conditions(stage_value)
                query += f" WHERE {where_clause}"

            # Process a '$group' stage
            elif stage_name == "$group":
                # Create a 'GROUP BY' clause from the '_id' key in the stage
                if isinstance(stage_value["_id"], dict):
                    group_keys = ', '.join([f"{key}" for key in stage_value["_id"].keys()])
                else:
                    group_keys = stage_value["_id"]

                # Create a list of aggregation expressions from the other keys in the stage
                agg_exprs = ', '.join(
                    [f"{op.upper()}({field.lstrip('$') if isinstance(field, str) else field}) as {alias}" for
                     alias, op_field in stage_value.items() if alias != '_id' for
                     op, field in op_field.items()])

                # Build the query string for the stage
                query = f"SELECT {group_keys}, {agg_exprs} FROM ({query}) as subquery GROUP BY {group_keys}"

            # Process a '$project' stage
            elif stage_name == "$project":
                # Create a 'SELECT' clause from the stage
                project_fields = ', '.join(
                    [f"{field.split('.')[-1]} as {alias}" if field.startswith('_id.') else f"{field} as {alias}" for
                     field, alias in stage_value.items()])

                # Build the query string for the stage
                query = f"SELECT {project_fields} FROM ({query}) as subquery"

            # Process a '$sort' stage
            elif stage_name == "$sort":
                # Create an 'ORDER BY' clause from the stage
                sort_fields = ', '.join(
                    [f"{field} {'ASC' if order > 0 else 'DESC'}" for field, order in stage_value.items()])

                # Build the query string for the stage
                query = f"SELECT * FROM ({query}) as subquery ORDER BY {sort_fields}"

            else:
                raise NotImplementedError(f"Aggregation stage '{stage_name}' is not supported.")

        # Execute the query and fetch all rows from the result
        cursor.execute(query)
        results = cursor.fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Convert the query result to a list of dictionaries, with Decimal values converted to integers
        return [dict(zip(column_names, map(decimal_to_int, result))) for result in results]


