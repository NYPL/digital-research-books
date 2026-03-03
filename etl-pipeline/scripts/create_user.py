"""
Script to create a new user in the database with hashed password.

This script prompts for username and password interactively, hashes the password
using scrypt, and inserts the user into the database.
"""

import os
from hashlib import scrypt
from getpass import getpass

from managers import DBManager
from logger import create_log

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, StatementError

logger = create_log(__file__)


def create_user_interactive(*args):
    """
    Create a new user in the database with interactive input.

    run via `main.py -sc create_user -e <target_env>`

    <target_env> should be a env file stem configured with details for the DB
    with a `users` table you want to write to (e.g. local-qa, qa, production).

    Script will error if username already exists due to uniqueness constraint
    on `users` table. See "models/postgres/user.py"

    Prompts for username and password, hashes the password using scrypt,
    and inserts the user record into the database.
    """
    # Get user input
    username = input("Enter username: ").strip()
    if not username:
        print("Error: Username cannot be empty")
        return

    password_input = getpass("Enter password: ").strip()
    if not password_input:
        print("Error: Password cannot be empty")
        return

    password_confirm = getpass("Confirm password: ").strip()
    if password_input != password_confirm:
        print("Error: Passwords do not match")
        return

    # Encrypt password: Generate salt and hash password
    salt = os.urandom(16)
    password_hash = scrypt(password_input.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)

    print("\nGenerating user credentials...")
    print(f"Password Hex: {password_hash.hex()}")
    print(f"Salt Hex: {salt.hex()}")

    create_user(username, password_input)
    print(f"\nUser '{username}' created successfully!")


def create_user(username: str, password: str):
    """
    Hash the given password and insert the user into the database.

    Extracted from create_user() to allow programmatic use (e.g. in test fixtures)
    without requiring interactive input.

    Args:
        username: The username to create.
        password: The plaintext password to hash and store.
    """
    logger.info(f'Inserting username "{username}" and password into user table.')
    salt = os.urandom(16)
    password_hash = scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)

    # Insert user into database
    engine = DBManager().generate_engine()

    # NOTE: password_hash is a bytes obj we are converting to hex (via <bytes>.hex()) \
    # and then converting back to binary via postgres decode(<hex>, 'hex'). \
    # Q: why not just directly write the bytes object?
    query = text("""
    INSERT INTO users (
        "user",
        "password",
        "salt",
        "date_created",
        "date_modified"
    )
    VALUES (
        :username,
        decode(:password_hex, 'hex'),
        decode(:salt_hex, 'hex'),
        NOW(),
        NOW()
    )
    """)

    try:
        with engine.connect() as cursor:
            cursor.execute(
                query,
                {
                    "username": username,
                    "password_hex": password_hash.hex(),
                    "salt_hex": salt.hex(),
                },
            )
            cursor.commit()
        logger.info(
            f'Succeeded at inserting username "{username}" and password into user table.'
        )
    except Exception as e:
        # Disable display of password_hex in traceback
        if isinstance(e, (DBAPIError, StatementError)):
            e.hide_parameters = True
        logger.exception(f"\nError creating user: {e}")
        raise e
