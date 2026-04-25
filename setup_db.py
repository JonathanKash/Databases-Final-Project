import sys
import random
from mariadb import connect, ProgrammingError
from MARIADB_CREDS import DB_CONFIG


def setup_db(data_dir="tpcds_data", verbose=True, parent_cur=None, parent_conn=None):
    try:
        # Use parent connection if provided (for tests), otherwise create our own
        if parent_cur is None and parent_conn is None:
            username = DB_CONFIG["username"]
            password = DB_CONFIG["password"]
            host = DB_CONFIG["host"]
            port = DB_CONFIG["port"]

            print(f"\nUsing:\n\tUsername: {username}\n\tPassword: {password}\n\tPort: {port}\n\tData Directory: {data_dir}")

            conn = connect(user=username, password=password, host=host, port=port, local_infile=True)
            cur = conn.cursor()
        else:
            cur = parent_cur
            conn = parent_conn

        database = DB_CONFIG["database"]
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"USE {database}")

        if verbose:
            print()
            print("Connected to the DB")

        # ---- Step 1: Create all 8 tables ----
        if verbose:
            print("Creating tables...")

        # Drop tables in reverse dependency order to avoid FK issues
        cur.execute("DROP TABLE IF EXISTS waitlist")
        cur.execute("DROP TABLE IF EXISTS rental_history")
        cur.execute("DROP TABLE IF EXISTS rental")
        cur.execute("DROP TABLE IF EXISTS store_sales")
        cur.execute("DROP TABLE IF EXISTS date_dim")
        cur.execute("DROP TABLE IF EXISTS customer")
        cur.execute("DROP TABLE IF EXISTS customer_address")
        cur.execute("DROP TABLE IF EXISTS item")

        # TPC-DS tables
        cur.execute("""
            CREATE TABLE item (
                i_item_sk        INTEGER      NOT NULL,
                i_item_id        CHAR(16)     NOT NULL,
                i_rec_start_date DATE,
                i_product_name   CHAR(50),
                i_brand          CHAR(50),
                i_class          CHAR(50),
                i_category       CHAR(50),
                i_manufact       CHAR(50),
                i_current_price  DECIMAL(7,2),
                i_num_owned      INTEGER      NOT NULL DEFAULT 1,
                PRIMARY KEY (i_item_sk)
            )
        """)

        cur.execute("""
            CREATE TABLE customer_address (
                ca_address_sk    INTEGER     NOT NULL,
                ca_street_number CHAR(10),
                ca_street_name   VARCHAR(60),
                ca_city          VARCHAR(60),
                ca_state         CHAR(2),
                ca_zip           CHAR(10),
                PRIMARY KEY (ca_address_sk)
            )
        """)

        cur.execute("""
            CREATE TABLE customer (
                c_customer_sk     INTEGER  NOT NULL,
                c_customer_id     CHAR(16) NOT NULL,
                c_first_name      CHAR(20),
                c_last_name       CHAR(30),
                c_email_address   CHAR(50),
                c_current_addr_sk INTEGER,
                PRIMARY KEY (c_customer_sk),
                FOREIGN KEY (c_current_addr_sk) REFERENCES customer_address(ca_address_sk)
            )
        """)

        cur.execute("""
            CREATE TABLE store_sales (
                ss_sold_date_sk  INTEGER,
                ss_item_sk       INTEGER      NOT NULL,
                ss_customer_sk   INTEGER,
                ss_ticket_number INTEGER      NOT NULL,
                ss_net_paid      DECIMAL(7,2),
                PRIMARY KEY (ss_item_sk, ss_ticket_number)
            )
        """)

        cur.execute("""
            CREATE TABLE date_dim (
                d_date_sk   INTEGER NOT NULL,
                d_date      DATE,
                PRIMARY KEY (d_date_sk)
            )
        """)

        # Operational tables
        cur.execute("""
            CREATE TABLE rental (
                item_id       CHAR(16) NOT NULL,
                customer_id   CHAR(16) NOT NULL,
                rental_date   DATE     NOT NULL,
                due_date      DATE     NOT NULL,
                PRIMARY KEY (item_id, customer_id)
            )
        """)

        cur.execute("""
            CREATE TABLE rental_history (
                item_id       CHAR(16) NOT NULL,
                customer_id   CHAR(16) NOT NULL,
                rental_date   DATE     NOT NULL,
                due_date      DATE     NOT NULL,
                return_date   DATE     NOT NULL,
                PRIMARY KEY (item_id, customer_id, rental_date)
            )
        """)

        cur.execute("""
            CREATE TABLE waitlist (
                item_id       CHAR(16) NOT NULL,
                customer_id   CHAR(16) NOT NULL,
                place_in_line INTEGER  NOT NULL,
                PRIMARY KEY (item_id, customer_id)
            )
        """)

        if verbose:
            print("Tables created successfully")

        # ---- Step 2: Load CSV files ----
        # Ensure data_dir path ends without trailing slash for consistency
        data_dir = data_dir.rstrip("/")

        load_order = [
            ("item", "item.csv",
             "(i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price)"),
            ("customer_address", "customer_address.csv",
             "(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip)"),
            ("customer", "customer.csv",
             "(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk)"),
            ("date_dim", "date_dim.csv",
             "(d_date_sk, d_date)"),
            ("store_sales", "store_sales.csv",
             "(ss_sold_date_sk, ss_item_sk, ss_customer_sk, ss_ticket_number, ss_net_paid)"),
        ]

        for table_name, csv_file, columns in load_order:
            file_path = f"{data_dir}/{csv_file}"
            if verbose:
                if table_name == "store_sales":
                    print(f"Loading {csv_file} (this may take several minutes)...")
                else:
                    print(f"Loading {csv_file}...")

            cur.execute(f"""
                LOAD DATA LOCAL INFILE '{file_path}'
                INTO TABLE {table_name}
                FIELDS TERMINATED BY '|'
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 LINES
                {columns}
            """)

            if verbose:
                print(f"  Loaded {cur.rowcount} rows into {table_name}")

        conn.commit()

        # ---- Step 3: Set i_num_owned to random value 1-5 ----
        if verbose:
            print("Setting i_num_owned to random values (1-5)...")

        cur.execute("SELECT i_item_sk FROM item")
        item_sks = [row[0] for row in cur.fetchall()]

        for sk in item_sks:
            cur.execute("UPDATE item SET i_num_owned = ? WHERE i_item_sk = ?", (random.randint(1, 5), sk))

        conn.commit()

        if verbose:
            print(f"  Updated {len(item_sks)} items")

        # ---- Step 4: Populate rental_history from store_sales ----
        if verbose:
            print("Populating rental_history from store_sales...")

        cur.execute("""
            INSERT IGNORE INTO rental_history (item_id, customer_id, rental_date, due_date, return_date)
            SELECT
                i.i_item_id,
                c.c_customer_id,
                dd.d_date                              AS rental_date,
                DATE_ADD(dd.d_date, INTERVAL 14 DAY)   AS due_date,
                DATE_ADD(dd.d_date, INTERVAL 14 DAY)   AS return_date
            FROM store_sales ss
            JOIN item         i  ON ss.ss_item_sk      = i.i_item_sk
            JOIN customer     c  ON ss.ss_customer_sk  = c.c_customer_sk
            JOIN date_dim     dd ON ss.ss_sold_date_sk = dd.d_date_sk
            WHERE ss.ss_customer_sk  IS NOT NULL
              AND ss.ss_sold_date_sk IS NOT NULL
        """)

        if verbose:
            print(f"  Inserted {cur.rowcount} rows into rental_history")

        conn.commit()

        if verbose:
            print()
            print("Database setup complete!")

        # Clean up if we created our own connection
        if parent_cur is None and parent_conn is None:
            cur.close()
            conn.close()

        return True

    except ProgrammingError as e:
        if verbose:
            print("Error:", e)
        return False

    except FileNotFoundError as e:
        if verbose:
            print(f"Could not find data directory: {e}")
        return False


def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "tpcds_data"
    # Strip trailing slash
    if data_dir.endswith("/"):
        data_dir = data_dir[:-1]

    success = setup_db(data_dir=data_dir)

    if success:
        print("Successfully loaded in the data")
    else:
        print("Failed to set up the database")


if __name__ == "__main__":
    main()
