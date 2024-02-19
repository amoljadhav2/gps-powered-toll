import psycopg2
from psycopg2 import Error
from python.util.pg_connect import connect_to_db

def update_balances(vehicle_reg_num, toll_roll_id, total_toll_per_travel):
    # Connect to the PostgreSQL database
    try:
       # Connect to the PostgreSQL database
       print("Step 1")
       connection = connect_to_db()
       print("Step 2")
       cursor = connection.cursor()
       print("Step 3")
       # Get user_id and balance from user_accounts table
       cursor.execute("""SELECT user_id, balance
                         FROM users_account
                         WHERE vehicle_reg_num = %s""", (vehicle_reg_num,))
       user_data = cursor.fetchone()
       user_id, user_balance = user_data[0], user_data[1]
       # Update user balance
       new_user_balance = user_balance - total_toll_per_travel
       cursor.execute("""UPDATE users_account
                         SET balance = %s
                         WHERE user_id = %s""", (new_user_balance, user_id))
       # Get vendor_id and balance from vendors table
       cursor.execute("""SELECT vendor_id, balance
                         FROM vendors_account
                         WHERE toll_roll_id = %s""", (toll_roll_id,))
       vendor_data = cursor.fetchone()
       vendor_id, vendor_balance = vendor_data[0], vendor_data[1]
       # Update vendor balance
       new_vendor_balance = vendor_balance + total_toll_per_travel
       cursor.execute("""UPDATE vendors_account
                         SET balance = %s
                         WHERE vendor_id = %s""", (new_vendor_balance, vendor_id))

       # Transaction successful, commit changes
       connection.commit()
       print("Transaction successful.")
    except (Exception, Error) as error:
       print("Error while connecting to PostgreSQL:", error)
    finally:
       # Close database connection
       if connection:
           """cursor.close()"""
           connection.close()

def main():
    update_balances("ABC123", 123, 10.00)

if __name__ == "__main__":
    main()

